package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.env.Enums;
import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.vendor.task.GssWrapper;
import io.netty.buffer.ByteBuf;
import org.ietf.jgss.*;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * @see PgConnectionTask
 */
final class GssUnitTask extends PostgreUnitTask {

    /**
     * @see #GssUnitTask(PgConnectionTask, Consumer)
     */
    static GssUnitTask encryption(PgConnectionTask task, Consumer<GssWrapper> gssWrapperConsumer) {
        return new GssUnitTask(task, gssWrapperConsumer);
    }

    /**
     * @see #GssUnitTask(PgConnectionTask)
     */
    static GssUnitTask authentication(PgConnectionTask task) {
        return new GssUnitTask(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(GssUnitTask.class);

    private final Consumer<GssWrapper> gssWrapperConsumer;

    private final boolean encryption;

    private Phase phase;

    private LoginContext loginContext;

    private GSSContext gssContext;

    /**
     * @see #authentication(PgConnectionTask)
     */
    private GssUnitTask(PgConnectionTask task) {
        super(task);
        this.gssWrapperConsumer = PgFunctions.noActionConsumer();
        this.encryption = false;
    }

    /**
     * @see #encryption(PgConnectionTask, Consumer)
     */
    private GssUnitTask(PgConnectionTask task, Consumer<GssWrapper> gssWrapperConsumer) {
        super(task);
        this.gssWrapperConsumer = gssWrapperConsumer;
        this.encryption = true;
    }


    @Override
    public final Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher;
        if (this.phase == null) {
            if (this.encryption) {
                publisher = Mono.just(createGssEncRequestMessage());
                this.phase = Phase.READ_ENC_RESPONSE;
            } else {
                publisher = null;
            }
        } else {
            // maybe reconnect
            publisher = null;
        }
        return publisher;
    }

    @Override
    public final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false;
        switch (this.phase) {
            case READ_ENC_RESPONSE:
                taskEnd = readGssEncResponse(cumulateBuffer);
                break;
            case INIT_GSS_CONTEXT: {
                try {
                    if (this.encryption) {
                        taskEnd = continueEncryptInitSecurityContext(cumulateBuffer);
                    } else {
                        taskEnd = continueAuthenticateInitSecurityContext(cumulateBuffer);
                    }
                } catch (Throwable e) {
                    addException(wrapAsGssException(notRequiredGss(), e));
                }
            }
            break;
            default:
                throw new IllegalStateException(String.format("Error this.phase[%s]", this.phase));
        }
        if (taskEnd) {
            this.phase = Phase.END;
        }
        return taskEnd;
    }

    @Override
    public final boolean hasOnePacket(ByteBuf cumulateBuffer) {
        return false;
    }


    /*################################## blow private method ##################################*/


    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     * @see #createGssEncRequestMessage()
     * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.12">GSSAPI Session Encryption</a>
     */
    private boolean readGssEncResponse(ByteBuf cumulateBuffer) {
        final int messageType = cumulateBuffer.readChar();
        final boolean notRequiredGss;
        notRequiredGss = notRequiredGss();
        boolean taskEnd;
        switch (messageType) {
            case 'E': {
                LOG.debug("Postgre server response 'E' not support GSS encryption.");
                // postgre demand reconnect for continue.
                addException(createNotSupportGssEncryption(true));
                taskEnd = true;
            }
            break;
            case 'N': {
                LOG.debug("Postgre server response 'N' not support GSS encryption.");
                addException(createNotSupportGssEncryption(notRequiredGss));
                taskEnd = true;
            }
            break;
            case 'G': {
                LOG.debug("Postgre server support GSS, build GSS context for connection.");
                try {
                    taskEnd = startupGssContext();
                } catch (Throwable cause) {
                    LOG.debug("build GSS context occur exception", cause);
                    taskEnd = true;
                    addException(wrapAsGssException(notRequiredGss, cause));
                }
            }
            break;
            default: {
                taskEnd = true;
                PgGssException e = new PgGssException(
                        notRequiredGss, "An error occurred while setting up the GSS Encoded connection.");
                addException(e);
            }
        }
        return taskEnd;
    }

    /**
     * @return true : task end.
     * @see #readGssEncResponse(ByteBuf)
     */
    private boolean startupGssContext() throws Throwable {
        //1. below obtain authenticated Subject
        Subject subject = tryGetAuthenticatedSubject();
        if (subject == null && this.properties.getOrDefault(PgKey0.jaasLogin, Boolean.class)) {
            subject = jaasLogin();
        }

        //2. create GSSContext
        final GSSContext gssContext;
        if (subject == null) {
            gssContext = createGssContext();
        } else {
            PrivilegedExceptionAction<GSSContext> action = this::createGssContext;
            gssContext = Subject.doAs(subject, action);
        }
        if (this.encryption) {
            gssContext.requestConf(true);
            gssContext.requestInteg(true);
        }

        final boolean taskEnd;
        // 3. init security context
        byte[] token = new byte[0];
        token = gssContext.initSecContext(token, 0, token.length);
        if (gssContext.isEstablished()) {
            taskEnd = true;
            LOG.debug("GssContext is established because last ConnectionTask.");
        } else if (token == null) {
            throw new PgGssException(false, "GssContext no established,but initiate token is null.");
        } else {
            final ByteBuf message;
            if (this.encryption) {
                // create GSS API message
                message = createGssApiMessage(token);
            } else {
                message = createGSSResponseMessage(token);
            }
            this.gssContext = gssContext;
            // send gss message to postgre server.
            this.sendPacket(Mono.just(message));
            // then ,see continueInitSecurityContext(ByteBuf) method.
            this.phase = Phase.INIT_GSS_CONTEXT;
            taskEnd = false;
        }

        return taskEnd;
    }

    /**
     * @return true : task end.
     * @see #decode(ByteBuf, Consumer)
     * @see #startupGssContext()
     */
    private boolean continueEncryptInitSecurityContext(ByteBuf cumulateBuffer) throws PgGssException {
        final GSSContext gssContext = Objects.requireNonNull(this.gssContext, "this.gssContext");

        byte[] token = new byte[cumulateBuffer.readInt()];
        cumulateBuffer.readBytes(token);
        try {
            token = gssContext.initSecContext(token, 0, token.length);
        } catch (GSSException e) {
            throw new PgGssException(notRequiredGss(), e.getMessage(), e);
        }

        if (token != null) {
            ByteBuf message = createGssApiMessage(token);
            // send token to postgre server.
            this.sendPacket(Mono.just(message));
        }
        final boolean taskEnd;
        if (gssContext.isEstablished()) {
            taskEnd = true;
            // invoke consumer for GSSAPI Session Encryption
            this.gssWrapperConsumer.accept(GssWrapper.wrap(this.loginContext, gssContext));
        } else {
            taskEnd = false;
        }
        return taskEnd;
    }

    /**
     * @return true : task end.
     * @see #startupGssContext()
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Message::AuthenticationGSSContinue</a>
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Message::ErrorResponse</a>
     */
    private boolean continueAuthenticateInitSecurityContext(final ByteBuf cumulateBuffer)
            throws PgGssException {
        final boolean taskEnd;
        final int type = cumulateBuffer.getChar(cumulateBuffer.readerIndex());
        switch (type) {
            case Messages.E: {
                addException(PgServerException.read(cumulateBuffer, this.adjutant.clientCharset()));
                taskEnd = true;
            }
            break;
            case Messages.R:
                taskEnd = handleAuthenticationGSSContinueMessage(cumulateBuffer);
                break;
            default:
                throw new PgGssException(notRequiredGss()
                        , "Postgre server error response of GSSResponse message, Session setup failed.");
        }
        return taskEnd;
    }


    private boolean notRequiredGss() {
        return !this.properties.getOrDefault(PgKey0.gssEncMode, Enums.GSSEncMode.class).requireEncryption();
    }

    /**
     * @see #startupGssContext()
     */
    private GSSContext createGssContext() throws GSSException {
        GSSManager manager = GSSManager.getInstance();
        PgHost0 hostInfo = this.adjutant.obtainHost();

        final Oid desiredMech;
        if (this.properties.getOrDefault(PgKey0.useSpnego, Boolean.class)
                && ConnectionTaskUtils.supportSpnego(manager)) {
            desiredMech = ConnectionTaskUtils.SPNEGO_MECHANISM;
        } else {
            desiredMech = ConnectionTaskUtils.KRB5_MECHANISM;
        }
        GSSName clientName = manager.createName(hostInfo.getUser(), GSSName.NT_USER_NAME, desiredMech);
        GSSName serverName = manager.createName(getServerPrincipalName(), GSSName.NT_HOSTBASED_SERVICE, desiredMech);

        final GSSCredential credential;
        credential = manager.createCredential(clientName, GSSContext.DEFAULT_LIFETIME
                , desiredMech, GSSCredential.INITIATE_ONLY);

        final GSSContext gssContext;
        gssContext = manager.createContext(serverName, desiredMech, credential, GSSContext.DEFAULT_LIFETIME);
        gssContext.requestMutualAuth(true);
        return gssContext;
    }

    /**
     * @see #startupGssContext()
     */
    @Nullable
    private Subject tryGetAuthenticatedSubject() {
        final Subject subject = Subject.getSubject(AccessController.getContext());
        if (subject == null) {
            return null;
        }
        Subject authenticated = null;
        final String serverPrincipalName = getServerPrincipalName();
        for (Object credential : subject.getPrivateCredentials()) {
            if (credential instanceof KerberosTicket
                    && ((KerberosTicket) credential).getServer().getName().equals(serverPrincipalName)) {
                authenticated = subject;
                break;
            }
        }
        return authenticated;
    }

    /**
     * @throws PgJdbdException login failure.
     * @see #startupGssContext()
     */
    private Subject jaasLogin() throws PgJdbdException {
        final String entryName = this.properties.get(PgKey0.jaasApplicationName, "pgjdbc");
        try {
            LoginContext lc = new LoginContext(entryName, this::jaasCallbackHandler);
            lc.login();
            this.loginContext = lc;
            return lc.getSubject();
        } catch (Throwable cause) {
            throw new JdbdException("JAAS Authentication failed", cause);
        }

    }

    /**
     * @see #jaasLogin()
     */
    private void jaasCallbackHandler(Callback[] callbackArray)
            throws java.io.IOException, UnsupportedCallbackException {
        //TODO zoro fill code
    }

    /**
     * @return database server kerberos principal name,that bases hose.
     */
    private String getServerPrincipalName() {
        return this.properties.get(PgKey0.kerberosServerName, "postgres") + "@"
                + this.adjutant.obtainHost().getHost();
    }


    /**
     * <p>
     * read AuthenticationGSSContinue message and send GSS api token.
     * </p>
     *
     * @return true:task end.
     * @see #continueAuthenticateInitSecurityContext(ByteBuf)
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Message::AuthenticationGSSContinue</a>
     */
    private boolean handleAuthenticationGSSContinueMessage(ByteBuf cumulateBuffer) {
        if (cumulateBuffer.readChar() != Messages.R) {
            throw new IllegalArgumentException("Not AuthenticationGSSContinue message.");
        }
        final GSSContext gssContext = Objects.requireNonNull(this.gssContext, "this.gssContext");
        byte[] token = new byte[cumulateBuffer.readInt()];
        cumulateBuffer.readBytes(token);
        try {
            token = gssContext.initSecContext(token, 0, token.length);
        } catch (GSSException e) {
            throw new PgGssException(notRequiredGss(), e.getMessage(), e);
        }
        if (token != null) {
            // send token to postgre server.
            sendPacket(Mono.just(createGSSResponseMessage(token)));
        }
        return gssContext.isEstablished();
    }


    /**
     * @see #startupGssContext()
     * @see #continueEncryptInitSecurityContext(ByteBuf)
     */
    private ByteBuf createGssApiMessage(byte[] token) {
        final ByteBuf message = this.adjutant.allocator().buffer(4 + token.length);
        message.writeInt(token.length);
        message.writeBytes(token);
        return message;
    }

    /**
     * @see #start()
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Message::GSSENCRequest</a>
     */
    private ByteBuf createGssEncRequestMessage() {
        final ByteBuf message = this.adjutant.allocator().buffer(8);
        // For historical reasons, the very first message sent by the client (the startup message) has no initial message-type byte.
        message.writeInt(8);
        message.writeShort(1234);
        message.writeShort(5680);
        return message;
    }

    /**
     * <p>
     * create GSSResponse message.
     * </p>
     *
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Message::GSSResponse</a>
     * @see #startupGssContext()
     * @see #continueAuthenticateInitSecurityContext(ByteBuf)
     */
    private ByteBuf createGSSResponseMessage(byte[] token) {
        ByteBuf message = this.adjutant.allocator().buffer(6 + token.length);
        message.writeChar('p');
        message.writeInt(4 + token.length);
        message.writeBytes(token);
        return message;
    }

    /**
     * @see #readGssEncResponse(ByteBuf)
     */
    private static PgGssException createNotSupportGssEncryption(boolean reconnect) {
        return new PgGssException(reconnect, "The server does not support GSS Encoding.");
    }

    /**
     * @see #readGssEncResponse(ByteBuf)
     */
    private static PgGssException wrapAsGssException(boolean reconnect, Throwable cause) {
        final PgGssException e;
        if (cause instanceof PgGssException) {
            e = (PgGssException) cause;
        } else {
            e = new PgGssException(reconnect, cause.getMessage(), cause);
        }
        return e;
    }


    private enum Phase {
        READ_ENC_RESPONSE,
        INIT_GSS_CONTEXT,
        END
    }


}
