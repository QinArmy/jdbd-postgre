package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.config.PropertyException;
import io.jdbd.postgre.PostgreReConnectableException;
import io.jdbd.postgre.ServerVersion;
import io.jdbd.postgre.config.Enums;
import io.jdbd.postgre.config.PGKey;
import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.util.PostgreExceptions;
import io.jdbd.postgre.util.PostgreTimes;
import io.jdbd.vendor.task.ChannelEncryptor;
import io.jdbd.vendor.task.ConnectionTask;
import io.jdbd.vendor.task.GssWrapper;
import io.jdbd.vendor.task.SslWrapper;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.function.Consumer;


/**
 * @see <a href="https://www.postgresql.org/docs/11/protocol-flow.html#id-1.10.5.7.3">Protocol::Start-up</a>
 * @see <a href="https://www.postgresql.org/docs/11/protocol-message-formats.html">Message::StartupMessage (F)</a>
 */
final class PostgreConnectionTask extends PostgreTask implements ConnectionTask {

    static Mono<AuthResult> authenticate(TaskAdjutant adjutant) {
        return Mono.create(sink -> {

            try {
                PostgreConnectionTask task = new PostgreConnectionTask(sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PostgreExceptions.wrap(e));
            }

        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(PostgreConnectionTask.class);


    private final MonoSink<AuthResult> sink;

    private ChannelEncryptor channelEncryptor;

    private Consumer<Void> reconnectConsumer;

    private GssWrapper gssWrapper;

    private Phase phase;

    private PostgreConnectionTask(MonoSink<AuthResult> sink, TaskAdjutant adjutant) {
        super(adjutant);
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
        final List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            return false;
        }

        boolean reconnect = false;
        final Iterator<JdbdException> iterator = errorList.listIterator();
        JdbdException error;
        while (iterator.hasNext()) {
            error = iterator.next();
            if (!(error instanceof PostgreReConnectableException)) {
                continue;
            }
            PostgreReConnectableException e = (PostgreReConnectableException) error;
            if (e.isReconnect()) {
                iterator.remove();
                reconnect = true;
                break;
            }
        }
        return reconnect;
    }



    /*################################## blow protected template method ##################################*/

    @Nullable
    @Override
    protected final Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher;
        if (this.properties.getOrDefault(PGKey.gssEncMode, Enums.GSSEncMode.class).needGssEnc()) {
            PostgreUnitTask unitTask;
            unitTask = GssUnitTask.encryption(this, this::configGssContext);
            this.unitTask = unitTask;
            publisher = unitTask.start();
            this.phase = Phase.GSS_ENCRYPTION_TASK;
        } else if (this.properties.getOrDefault(PGKey.sslmode, Enums.SslMode.class).needSslEnc()) {
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
        if (unitTask != null && !unitTask.decode(cumulateBuffer, serverStatusConsumer)) {
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
                    taskEnd = readStartUpResponse(cumulateBuffer, serverStatusConsumer);

                }
                break;
                default:

            }

        }

        return taskEnd;
    }

    /*################################## blow private method ##################################*/

    private boolean readStartUpResponse(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        return false;
    }

    /**
     * @see #start()
     */
    private void configGssContext(GssWrapper gssWrapper) {
        this.gssWrapper = gssWrapper;
        this.channelEncryptor.addGssContext(gssWrapper);
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
                && this.properties.getOrDefault(PGKey.sslmode, Enums.SslMode.class).needSslEnc()) {
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
        List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            return false;
        }
        boolean has = false;
        for (Throwable error : errorList) {
            if (!(error instanceof PostgreReConnectableException)) {
                continue;
            }
            PostgreReConnectableException e = (PostgreReConnectableException) error;
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
     * @see #decode(ByteBuf, Consumer)
     * @see #startSslEncryptionUnitTask()
     */
    private boolean handleSslEncryptionTaskEnd() {
        return false;
    }


    /**
     * <p>
     * create startup message and modify {@link #phase} to {@link Phase#READ_START_UP_RESPONSE}.
     * </p>
     *
     * @throws PropertyException property can't convert
     * @see #start()
     * @see #handleGssEncryptionTaskEnd()
     * @see #handleSslEncryptionTaskEnd()
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Protocol::StartupMessage</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-MESSAGE-CONCEPTS">Messaging Overview</a>
     */
    private Publisher<ByteBuf> startStartupMessage() {
        final ByteBuf message = this.adjutant.allocator().buffer(1024);
        // For historical reasons, the very first message sent by the client (the startup message) has no initial message-type byte.
        message.writeZero(4); // length placeholder

        message.writeShort(3); // protocol major
        message.writeShort(0); // protocol major
        for (Pair<String, String> pair : obtainStartUpParamList()) {
            Messages.writeString(message, pair.getFirst());
            Messages.writeString(message, pair.getSecond());
        }
        message.writeByte(0);// Terminating \0

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
        minVersion = this.properties.getProperty(PGKey.assumeMinServerVersion, ServerVersion.class
                , ServerVersion.INVALID);

        List<Pair<String, String>> list = new ArrayList<>();

        list.add(new Pair<>("user", host.getUser()));
        list.add(new Pair<>("database", host.getNonNullDbName()));
        list.add(new Pair<>("client_encoding", Messages.CLIENT_CHARSET.name()));
        list.add(new Pair<>("DateStyle", "ISO"));

        list.add(new Pair<>("TimeZone", PostgreTimes.systemZoneOffset().normalized().getId()));
        list.add(new Pair<>("jdbd", getDriverName()));

        if (minVersion.compareTo(ServerVersion.V9_0) >= 0) {
            list.add(new Pair<>("extra_float_digits", "3"));
            list.add(new Pair<>("application_name", this.properties.getOrDefault(PGKey.ApplicationName)));
        } else {
            list.add(new Pair<>("extra_float_digits", "2"));
        }

        if (minVersion.compareTo(ServerVersion.V9_4) >= 0) {
            String replication = this.properties.getProperty(PGKey.replication);
            if (replication != null) {
                list.add(new Pair<>("replication", replication));
            }
        }

        final String currentSchema = this.properties.getProperty(PGKey.currentSchema);
        if (currentSchema != null) {
            list.add(new Pair<>("search_path", currentSchema));
        }
        final String options = this.properties.getProperty(PGKey.options);
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
            LOG.debug("StartupPacket[{}]", builder);
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * @see #start()
     */
    private void handleError(Throwable e) {
        this.phase = Phase.DISCONNECT;
        this.sink.error(PostgreExceptions.wrap(e));

    }

    /**
     * @see #obtainStartUpParamList()
     */
    private String getDriverName() {
        String driverName = ClientProtocol.class.getPackage().getImplementationVersion();
        if (driverName == null) {
            driverName = "jdbd-postgre-test";
        }
        return driverName;
    }

    private void assertPhase(Phase expected) {
        if (this.phase != expected) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't expected[%s]", this.phase, expected));
        }
    }




    /*################################## blow private static method ##################################*/


    private enum Phase {
        SSL_ENCRYPTION_TASK,
        READ_START_UP_RESPONSE,
        GSS_ENCRYPTION_TASK,
        DISCONNECT
    }

}
