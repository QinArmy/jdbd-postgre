package io.jdbd.postgre.protocol.client;

import io.jdbd.config.PropertyException;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgReConnectableException;
import io.jdbd.postgre.ServerVersion;
import io.jdbd.postgre.config.Enums;
import io.jdbd.postgre.config.PgKey;
import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.task.*;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Consumer;


/**
 * @see <a href="https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.3">Protocol::Start-up</a>
 * @see <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Message::StartupMessage (F)</a>
 */
final class PgConnectionTask extends PgTask implements ConnectionTask {

    static Mono<AuthResult> authenticate(TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                PgConnectionTask task = new PgConnectionTask(sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }

        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(PgConnectionTask.class);


    private final MonoSink<AuthResult> sink;

    private ChannelEncryptor channelEncryptor;

    private Consumer<Void> reconnectConsumer;

    private GssWrapper gssWrapper;

    private AuthResult authResult;

    private Phase phase;


    private PgConnectionTask(MonoSink<AuthResult> sink, TaskAdjutant adjutant) {
        super(adjutant, sink::error);
        this.sink = sink;
    }

    /*################################## blow ConnectionTask method ##################################*/

    @Override
    public final void addSsl(Consumer<SslWrapper> sslConsumer) {
        //no-op
    }

    @Override
    public final boolean disconnect() {
        return this.phase == Phase.DISCONNECT;
    }

    @Override
    public final boolean reconnect() {
        final List<Throwable> errorList = this.errorList;
        if (errorList == null) {
            return false;
        }

        boolean reconnect = false;
        final Iterator<Throwable> iterator = errorList.listIterator();
        Throwable error;
        while (iterator.hasNext()) {
            error = iterator.next();
            if (!(error instanceof PgReConnectableException)) {
                continue;
            }
            PgReConnectableException e = (PgReConnectableException) error;
            if (e.isReconnect()) {
                iterator.remove();
                reconnect = true;
                break;
            }
        }
        return reconnect;
    }



    /*################################## blow protected template method ##################################*/


    @Override
    protected final Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher;
        if (this.properties.getOrDefault(PgKey.gssEncMode, Enums.GSSEncMode.class).needGssEnc()) {
            PostgreUnitTask unitTask;
            unitTask = GssUnitTask.encryption(this, null);
            this.unitTask = unitTask;
            publisher = unitTask.start();
            this.phase = Phase.GSS_ENCRYPTION_TASK;
        } else if (this.properties.getOrDefault(PgKey.sslmode, SslMode.class) != SslMode.DISABLED) {
            publisher = startSslEncryptionUnitTask();
        } else {
            publisher = startStartupMessage();
            this.phase = Phase.READ_START_UP_RESPONSE;
        }
        return publisher;
    }

    @Override
    protected final Action onError(Throwable e) {
        return Action.TASK_END;
    }

    @Override
    protected final boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final PostgreUnitTask unitTask = this.unitTask;
        if (unitTask == null) {
            if (!Messages.hasOneMessage(cumulateBuffer)) {
                return false;
            }
        } else if (!unitTask.hasOnePacket(cumulateBuffer) || !unitTask.decode(cumulateBuffer, serverStatusConsumer)) {
            return false;
        }
        boolean taskEnd = false, continueDecode = true;
        while (continueDecode) {
            switch (this.phase) {
                case GSS_ENCRYPTION_TASK: {
                    taskEnd = handleGssEncryptionTaskEnd();
                    continueDecode = false;
                }
                break;
                case SSL_ENCRYPTION_TASK: {
                    taskEnd = handleSslEncryptionTaskEnd(cumulateBuffer);
                    continueDecode = false;
                }
                break;
                case READ_START_UP_RESPONSE: {
                    taskEnd = readStartUpResponse(cumulateBuffer);
                    continueDecode = false;
                }
                break;
                case HANDLE_AUTHENTICATION: {
                    taskEnd = handleAuthenticationMethod(cumulateBuffer);
                    continueDecode = false;
                }
                break;
                case READ_MSG_AFTER_OK: {
                    taskEnd = readMessageAfterAuthenticationOk(cumulateBuffer);
                    continueDecode = false;
                }
                break;
                default: {

                }

            }

        }

        if (taskEnd) {
            if (hasError()) {
                this.phase = Phase.DISCONNECT;
                publishError(this.sink::error);
            } else {
                this.sink.success(Objects.requireNonNull(this.authResult, "this.authResult"));
            }
        }
        return taskEnd;
    }




    /*################################## blow private method ##################################*/

    /**
     * <p>
     * Read possible response of StartupMessage:
     *     <ul>
     *         <li>AuthenticationOk</li>
     *         <li>ErrorResponse</li>
     *         <li>AuthenticationMD5Password</li>
     *         <li>AuthenticationCleartextPassword</li>
     *         <li>AuthenticationKerberosV5</li>
     *         <li>AuthenticationSCMCredential</li>
     *         <li>AuthenticationGSS</li>
     *         <li>AuthenticationSSPI</li>
     *         <li>AuthenticationGSSContinue</li>
     *         <li>AuthenticationSASL</li>
     *         <li>AuthenticationSASLContinue</li>
     *         <li>AuthenticationSASLFinal</li>
     *         <li>NegotiateProtocolVersion</li>
     *     </ul>
     * </p>
     *
     * @return true : task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readStartUpResponse(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_START_UP_RESPONSE);
        boolean taskEnd = false;

        LOG.debug("Receive startup response.");
        switch (cumulateBuffer.getByte(cumulateBuffer.readerIndex())) {
            case Messages.E: {
                // error,server close connection.
                taskEnd = true;
                ErrorMessage error = ErrorMessage.read(cumulateBuffer, this.adjutant.clientCharset());
                addError(PgExceptions.createErrorException(error));
                LOG.debug("Read StartUpResponse error:{}", error);
            }
            break;
            case Messages.R: {
                LOG.debug("start handle authentication method.");
                this.phase = Phase.HANDLE_AUTHENTICATION;
                taskEnd = handleAuthenticationMethod(cumulateBuffer);
            }
            break;
            case Messages.v: {

            }
            break;
            default: {

            }
        }
        return taskEnd;
    }

    /**
     * @return true :  task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean handleAuthenticationMethod(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.HANDLE_AUTHENTICATION);
        final int msgIndex = cumulateBuffer.readerIndex();

        if (cumulateBuffer.getByte(msgIndex) != Messages.R) {
            throw createNonAuthenticationRequestError();
        }

        final int nextMsgIndex = msgIndex + 1 + cumulateBuffer.getInt(msgIndex + 1);
        final int authenticateMethod = cumulateBuffer.getInt(msgIndex + 5);
        boolean taskEnd;
        switch (authenticateMethod) {
            case Messages.AUTH_OK: {
                LOG.debug("Authentication success,receiving message from server.");
                this.phase = Phase.READ_MSG_AFTER_OK;
                cumulateBuffer.readerIndex(nextMsgIndex); // skip AuthenticationOk message.
                taskEnd = readMessageAfterAuthenticationOk(cumulateBuffer);
            }
            break;
            case Messages.AUTH_KRB5:
            case Messages.AUTH_CLEAR_TEXT:
                taskEnd = true;
                addError(new PgJdbdException("Not support authentication method. "));
                break;
            case Messages.AUTH_MD5: {
                taskEnd = handleMd5PasswordAuthentication(cumulateBuffer);
            }
            break;
            case Messages.AUTH_SCM:
            case Messages.AUTH_GSS:
            case Messages.AUTH_GSS_CONTINUE:
            case Messages.AUTH_SSPI:
            case Messages.AUTH_SASL:
            case Messages.AUTH_SASL_CONTINUE:
            case Messages.AUTH_SASL_FINAL:
            default: {
                taskEnd = true;
                String m = String.format("Client not support authentication method(%s).", authenticateMethod);
                addError(new PgJdbdException(m));
            }
        }
        return taskEnd;
    }


    /**
     * <p>
     * send PasswordMessage
     * </p>
     *
     * @return true : task end.
     * @see #readStartUpResponse(ByteBuf)
     */
    private boolean handleMd5PasswordAuthentication(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.HANDLE_AUTHENTICATION);
        LOG.debug("start MD5 password authentication.");
        final int msgIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.R) {
            throw createNonAuthenticationRequestError();
        }
        final int nextMsgIndex = msgIndex + 1 + cumulateBuffer.readInt();
        if (cumulateBuffer.readInt() != Messages.AUTH_MD5) {
            throw new IllegalArgumentException("Non AuthenticationMD5Password message.");
        }
        boolean taskEnd = false;
        PostgreHost host = this.adjutant.obtainHost();
        final String password = host.getPassword();
        if (PgStrings.hasText(password)) {
            final byte[] salt = new byte[4];
            cumulateBuffer.readBytes(salt);
            // send md5 authentication  response.
            this.packetPublisher = Mono.just(
                    Messages.md5Password(host.getUser(), password, salt, this.adjutant.allocator())
            );
            LOG.debug("Send MD5 password authentication message.");
        } else {
            String m;
            m = "The server requested password-based authentication, but no password was provided.";
            taskEnd = true;
            addError(new PgJdbdException(m));
        }
        cumulateBuffer.readerIndex(nextMsgIndex);
        return taskEnd;
    }

    /**
     * @return true : task end.
     * @see #readStartUpResponse(ByteBuf)
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readMessageAfterAuthenticationOk(ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_MSG_AFTER_OK);
        if (!Messages.hasReadyForQuery(cumulateBuffer)) {
            return false;
        }
        // read ParameterStatus messages
        final Map<String, String> serverStatusMap = new HashMap<>();
        TxStatus txStatus = null;
        BackendKeyData keyData = null;
        NoticeMessage notice = null;

        final Charset clientCharset = this.adjutant.clientCharset();
        loop:
        while (Messages.hasOneMessage(cumulateBuffer)) {
            final int msgIndex = cumulateBuffer.readerIndex(), msgType = cumulateBuffer.readByte();
            final int nextMsgIndex = msgIndex + 1 + cumulateBuffer.readInt();

            switch (msgType) {
                case Messages.E: { // ErrorResponse message
                    ErrorMessage error = ErrorMessage.readBody(cumulateBuffer, nextMsgIndex, clientCharset);
                    addError(PgExceptions.createErrorException(error));
                }
                break loop;
                case Messages.S: {// ParameterStatus message
                    serverStatusMap.put(
                            Messages.readString(cumulateBuffer, clientCharset)
                            , Messages.readString(cumulateBuffer, clientCharset)
                    );
                }
                break;
                case Messages.Z: {// ReadyForQuery message
                    // here , session build success.
                    txStatus = TxStatus.from(cumulateBuffer.readByte());
                    LOG.debug("Session build success,transaction status[{}].", txStatus);
                }
                break loop;
                case Messages.K: {// BackendKeyData message
                    // modify server BackendKeyData
                    keyData = BackendKeyData.readBody(cumulateBuffer);
                }
                break;
                case Messages.N: { // NoticeResponse message
                    // modify server status.
                    notice = NoticeMessage.readBody(cumulateBuffer, nextMsgIndex, clientCharset);
                }
                break;
                default: { // Unknown message
                    String msg = String.format("Unknown message type [%s] after authentication ok.", (char) msgType);
                    addError(new PgJdbdException(msg));
                }
            }// switch
            cumulateBuffer.readerIndex(nextMsgIndex); // avoid tail filler.
        }
        this.authResult = new AuthResult(
                Collections.unmodifiableMap(serverStatusMap)
                , Objects.requireNonNull(keyData, "keyData")
                , Objects.requireNonNull(txStatus, "txStatus")
                , notice);
        return true;

    }


    /**
     * <p>
     *     <ol>
     *         <li>handle {@link GssUnitTask} end</li>
     *         <li>try start {@link SslUnitTask} or send  startup message</li>
     *         <li>maybe modify {@link #phase}</li>
     *     </ol>
     * </p>
     *
     * @return true : task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean handleGssEncryptionTaskEnd() {
        assertPhase(Phase.GSS_ENCRYPTION_TASK);

        Objects.requireNonNull(this.unitTask, "this.unitTask");
        this.unitTask = null;
        final boolean taskEnd;
        if (hasError()) {
            taskEnd = !hasReConnectableError();
        } else if (this.gssWrapper == null
                && this.properties.getOrDefault(PgKey.sslmode, Enums.SslMode.class).needSslEnc()) {
            taskEnd = false;
            this.packetPublisher = startSslEncryptionUnitTask();
        } else {
            taskEnd = false;
            this.packetPublisher = startStartupMessage();
        }
        return taskEnd;
    }

    /**
     * @see #reconnect()
     * @see #handleGssEncryptionTaskEnd()
     */
    private boolean hasReConnectableError() {
        List<Throwable> errorList = this.errorList;
        if (errorList == null) {
            return false;
        }
        boolean has = false;
        for (Throwable error : errorList) {
            if (!(error instanceof PgReConnectableException)) {
                continue;
            }
            PgReConnectableException e = (PgReConnectableException) error;
            if (e.isReconnect()) {
                has = true;
                break;
            }

        }
        return has;
    }


    /**
     * <p>
     * start ssl encryption task and modify {@link #phase} to {@link Phase#SSL_ENCRYPTION_TASK}
     * </p>
     *
     * @see #start()
     * @see #handleGssEncryptionTaskEnd()
     */
    @Nullable
    private Publisher<ByteBuf> startSslEncryptionUnitTask() {
        if (this.unitTask != null) {
            throw new IllegalStateException("this.unitTask non-null,reject start ssl unit task.");
        }
        SslUnitTask unitTask = SslUnitTask.ssl(this, this.channelEncryptor::addSsl);
        this.unitTask = unitTask;
        final Publisher<ByteBuf> publisher;
        publisher = unitTask.start();
        this.phase = Phase.SSL_ENCRYPTION_TASK;
        return publisher;
    }


    /**
     * <p>
     * create startup message and modify {@link #phase} to {@link Phase#READ_START_UP_RESPONSE}.
     * </p>
     *
     * @throws PropertyException property can't convert
     * @see #start()
     * @see #handleGssEncryptionTaskEnd()
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Protocol::StartupMessage</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-MESSAGE-CONCEPTS">Messaging Overview</a>
     */
    private Publisher<ByteBuf> startStartupMessage() {
        final ByteBuf message = this.adjutant.allocator().buffer(1024);
        // For historical reasons, the very first message sent by the client (the startup message) has no initial message-type byte.
        message.writeZero(4); // length placeholder

        message.writeShort(3); // protocol major
        message.writeShort(0); // protocol major

        final Charset charset = this.adjutant.clientCharset();
        for (Pair<String, String> pair : obtainStartUpParamList()) {
            message.writeBytes(pair.getFirst().getBytes(charset));
            message.writeByte(Messages.STRING_TERMINATOR);

            message.writeBytes(pair.getSecond().getBytes(charset));
            message.writeByte(Messages.STRING_TERMINATOR);
        }
        message.writeByte(Messages.STRING_TERMINATOR);// Terminating \0

        //no initial message-type byte
        final int readableBytes = message.readableBytes(), writerIndex = message.writerIndex();
        message.writerIndex(message.readerIndex());
        message.writeInt(readableBytes);
        message.writerIndex(writerIndex);

        this.phase = Phase.READ_START_UP_RESPONSE;
        return Mono.just(message);
    }


    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean handleSslEncryptionTaskEnd(ByteBuf cumulateBuffer) {
        return false;
    }


    /**
     * @return a unmodifiable list.
     * @see #startStartupMessage()
     */
    private List<Pair<String, String>> obtainStartUpParamList() {
        final PostgreHost host = this.adjutant.obtainHost();
        final ServerVersion minVersion;
        minVersion = this.properties.getProperty(PgKey.assumeMinServerVersion, ServerVersion.class
                , ServerVersion.INVALID);

        List<Pair<String, String>> list = new ArrayList<>();

        list.add(new Pair<>("user", host.getUser()));
        list.add(new Pair<>("database", host.getNonNullDbName()));
        list.add(new Pair<>("client_encoding", this.adjutant.clientCharset().name()));
        list.add(new Pair<>("DateStyle", "ISO")); // must be  ISO,because simplify program

        list.add(new Pair<>("TimeZone", PgTimes.systemZoneOffset().normalized().getId()));
        list.add(new Pair<>("IntervalStyle", "iso_8601"));// must be  ISO,because simplify program

        if (minVersion.compareTo(ServerVersion.V9_0) >= 0) {
            list.add(new Pair<>("extra_float_digits", "3"));
            list.add(new Pair<>("application_name", getApplicationName()));
        } else {
            list.add(new Pair<>("extra_float_digits", "2"));
        }
        if (minVersion.compareTo(ServerVersion.V9_4) >= 0) {
            String replication = this.properties.getProperty(PgKey.replication);
            if (replication != null) {
                list.add(new Pair<>("replication", replication));
            }
        }

        final String currentSchema = this.properties.getProperty(PgKey.currentSchema);
        if (currentSchema != null) {
            list.add(new Pair<>("search_path", currentSchema));
        }
        final String options = this.properties.getProperty(PgKey.options);
        if (options != null) {
            list.add(new Pair<>("search_path", options));
        }

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            int count = 0;
            final String separator = System.lineSeparator();
            for (Pair<String, String> pair : list) {
                if (count > 0) {
                    builder.append(",");
                }
                builder.append(separator)
                        .append(pair.getFirst())
                        .append("=")
                        .append(pair.getSecond());
                count++;
            }
            builder.append(separator);
            LOG.debug("StartupMessage[{}]", builder);
        }
        return Collections.unmodifiableList(list);
    }


    /**
     * @see #obtainStartUpParamList()
     */
    private String getApplicationName() {
        String applicationName = this.properties.getProperty(PgKey.ApplicationName);
        if (applicationName == null) {
            applicationName = ClientProtocol.class.getPackage().getImplementationVersion();
            if (applicationName == null) {
                applicationName = "jdbd-postgre-test";
            }
        }
        return applicationName;
    }

    private void assertPhase(Phase expected) {
        if (this.phase != expected) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't expected[%s]", this.phase, expected));
        }
    }


    private static IllegalArgumentException createNonAuthenticationRequestError() {
        return new IllegalArgumentException("Non authentication request message");
    }




    /*################################## blow private static method ##################################*/


    private enum Phase {
        SSL_ENCRYPTION_TASK,
        READ_START_UP_RESPONSE,
        HANDLE_AUTHENTICATION,
        READ_MSG_AFTER_OK,
        GSS_ENCRYPTION_TASK,
        DISCONNECT,
        END
    }

}
