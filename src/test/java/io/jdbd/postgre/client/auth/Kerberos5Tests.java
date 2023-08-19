package io.jdbd.postgre.client.auth;

import org.ietf.jgss.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.function.Consumer;

/**
 * kerberos5 unit tests
 */
public class Kerberos5Tests {

    private static final Logger LOG = LoggerFactory.getLogger(Kerberos5Tests.class);

    private static final boolean LOGIN_FILE_PRESENT;

    private static final Oid KRB5_MECHANISM;

    private static final int APPLICATION_SERVER_PORT = 8882;

    static {
        LOGIN_FILE_PRESENT = loadLoginFile();
        try {
            KRB5_MECHANISM = new Oid("1.3.6.1.5.5.2");
        } catch (GSSException e) {
            throw new RuntimeException(e);
        }

    }

    @BeforeClass
    public static void beforeClass() {
        if (!LOGIN_FILE_PRESENT) {
            throw new RuntimeException("Not found login file.");
        }
    }

    /**
     * <p>
     * This unit test need config local KDC
     * </p>
     */
    @Test
    public void gssKerberos5Client() throws Exception {

        final String entryName = "JaasClientUnitTest";
        LoginContext loginContext = new LoginContext(entryName, this::kerberos5ClientCallBack);

        loginContext.login();
        Subject subject = loginContext.getSubject();
        LOG.info("publicCredentials:{}", subject.getPublicCredentials());
        for (Object credential : subject.getPrivateCredentials()) {
            LOG.info("credential java type:{}", credential.getClass().getName());
            LOG.info("privateCredential:{}", credential);
        }
        PrivilegedExceptionAction<GSSContext> action = this::createClientCredential;
        clientGssTask(Subject.doAs(subject, action));
        loginContext.logout();
    }

    @Test
    public void gssKerberos5Service() throws Exception {
        final String entryName = "JaasServiceUnitTest";
        LoginContext loginContext = new LoginContext(entryName, this::kerberos5ServiceCallBack);

        loginContext.login();
        Subject subject = loginContext.getSubject();

        PrivilegedExceptionAction<Void> action = this::serviceAction;
        Subject.doAs(subject, action);

        loginContext.logout();

    }


    @Test
    public void kerberos5Client() throws Exception {
        final String entryName = "JaasClientUnitTest";
        LoginContext loginContext = new LoginContext(entryName, this::kerberos5ClientCallBack);
        loginContext.login();

        Subject subject = loginContext.getSubject();

        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof KerberosPrincipal) {
                KerberosPrincipal p = (KerberosPrincipal) principal;
                LOG.info("principal:{}", p.getName());
            }


        }
        for (Object credential : subject.getPrivateCredentials()) {
            LOG.info("credential java type:{}", credential.getClass().getName());
            LOG.info("privateCredential:{}", credential);
        }
        loginContext.logout();
    }


    /*################################## blow private method ##################################*/


    /**
     * @see #gssKerberos5Service()
     */
    private Void serviceAction() throws Exception {
        GSSManager manager = GSSManager.getInstance();
        final GSSName serviceName;

        serviceName = manager.createName("postgre@localhost", GSSName.NT_HOSTBASED_SERVICE);
        GSSCredential serverCreds = manager.createCredential(serviceName, GSSCredential.INDEFINITE_LIFETIME,
                KRB5_MECHANISM,
                GSSCredential.ACCEPT_ONLY);
        ServerSocket ss = new ServerSocket(APPLICATION_SERVER_PORT);
        Socket socket;
        LOG.info("Waiting for incoming connection...");
        socket = ss.accept();
        LOG.info("income connection.");

        DataInputStream inStream = new DataInputStream(socket.getInputStream());
        DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());

        GSSContext context = manager.createContext(serverCreds);

        while (!context.isEstablished()) {
            byte[] token = new byte[inStream.readInt()];
            if (token.length == 0) {
                LOG.debug("skipping zero length token");
                continue;
            }
            inStream.readFully(token);
            LOG.info("accept {} bytes token.", token.length);
            token = context.acceptSecContext(token, 0, token.length);
            if (token != null) {
                LOG.info("send {} bytes token.", token.length);
                outStream.writeInt(token.length);
                outStream.write(token, 0, token.length);
                outStream.flush();
            }
        }

        LOG.info("Context Established\nclient principal : {}\nservice principal : {}\nmutual authentication : {}"
                , context.getSrcName(), context.getTargName(), context.getMutualAuthState());

        final MessageProp prop = new MessageProp(0, false);

        int length;
        byte[] cipherMessage, plainMessage, message, timeMessage;
        String plantText;
        while (true) {
            length = inStream.readInt();
            if (length == 0) {
                continue;
            }
            cipherMessage = new byte[length];
            inStream.readFully(cipherMessage);

            plainMessage = context.unwrap(cipherMessage, 0, cipherMessage.length, prop);
            plantText = new String(plainMessage, StandardCharsets.UTF_8);

            LOG.info("Received plain message : {}", plantText);

            /*
             * First reset the QOP of the MessageProp to 0
             * to ensure the default Quality-of-Protection
             * is applied.
             */
            prop.setQOP(0);

            message = new byte[20 + plainMessage.length];
            timeMessage = LocalDateTime.now().toString().getBytes(StandardCharsets.UTF_8);
            System.arraycopy(timeMessage, 0, message, 0, timeMessage.length);
            System.arraycopy(plainMessage, 0, message, 20, plainMessage.length);

            LOG.info("output plain message : {}", new String(message, StandardCharsets.UTF_8));
            message = context.wrap(message, 0, message.length, prop);

            outStream.writeInt(message.length);
            outStream.write(message, 0, message.length);
            outStream.flush();

            if (plantText.equals("quit")) {
                break;
            }

        }
        context.dispose();
        socket.close();
        return null;
    }


    /**
     * @see #gssKerberos5Client()
     */
    private GSSContext createClientCredential() throws Exception {
        GSSManager manager = GSSManager.getInstance();
        final GSSName clientName = manager.createName("jdbd/client", GSSName.NT_USER_NAME);
        final GSSName serverName;
        serverName = manager.createName("postgre@localhost", GSSName.NT_HOSTBASED_SERVICE, KRB5_MECHANISM);
        final GSSCredential credential;
        credential = manager.createCredential(clientName, GSSContext.INDEFINITE_LIFETIME
                , KRB5_MECHANISM, GSSCredential.INITIATE_ONLY);
        final GSSContext gssContext;
        gssContext = manager.createContext(serverName, KRB5_MECHANISM, credential, GSSContext.INDEFINITE_LIFETIME);
        gssContext.requestMutualAuth(true);
        return gssContext;
    }

    /**
     * @see #gssKerberos5Client()
     */
    private Void clientGssTask(GSSContext gssContext) throws Exception {

        LOG.info("connect application server...");

        Socket socket = new Socket("localhost", APPLICATION_SERVER_PORT);
        LOG.info("connect application server success,start gss context...");

        final DataInputStream inStream = new DataInputStream(socket.getInputStream());
        final DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());

        byte[] token = new byte[0];
        while (true) {
            token = gssContext.initSecContext(token, 0, token.length);
            Subject subject = Subject.getSubject(AccessController.getContext());
            if (subject != null) {
                Set<KerberosTicket> ticketSet = subject.getPrivateCredentials(KerberosTicket.class);
                for (KerberosTicket ticket : ticketSet) {
                    LOG.info("ticket:{}", ticket.getServer().getName());
                }
            }

            if (token != null) {
                LOG.info("send {} bytes token.", token.length);
                outStream.writeInt(token.length);
                outStream.write(token);
                outStream.flush();
            }

            if (gssContext.isEstablished()) {
                break;
            }
            token = new byte[inStream.readInt()];
            if (token.length == 0) {
                continue;
            }
            inStream.readFully(token);
            LOG.info("Receive {} bytes.", token.length);

        }
        LOG.info("Context Established\nclient principal : {}\nservice principal : {}\nmutual authentication : {}"
                , gssContext.getSrcName(), gssContext.getTargName(), gssContext.getMutualAuthState());

        final MessageProp prop = new MessageProp(0, true);

        final Consumer<String> consumer = command -> {
            try {
                byte[] message;
                LOG.info("send command:{}", command);
                message = command.getBytes(StandardCharsets.UTF_8);
                message = gssContext.wrap(message, 0, message.length, prop);

                outStream.writeInt(message.length);
                outStream.write(message);
                outStream.flush();

                message = new byte[inStream.readInt()];
                inStream.readFully(message);
                message = gssContext.unwrap(message, 0, message.length, prop);
                LOG.info("Receive response:{}", new String(message, StandardCharsets.UTF_8));
            } catch (GSSException | IOException e) {
                throw new RuntimeException(e);
            }
        };
        consumer.accept("ping");
        consumer.accept("quit");

        gssContext.dispose();
        socket.close();
        return null;
    }


    private void kerberos5ClientCallBack(Callback[] callbacks) {
        for (Callback callback : callbacks) {
            if (callback instanceof PasswordCallback) {
                PasswordCallback p = (PasswordCallback) callback;
                p.setPassword("jdbd123".toCharArray());
                LOG.debug("call password");
            } else if (callback instanceof NameCallback) {
                NameCallback n = (NameCallback) callback;
                n.setName("jdbd/client");
                LOG.debug("call name");
            } else {
                throw new RuntimeException(MessageFormat.format("Not support {1}", callback.getClass().getName()));
            }
        }

    }

    /**
     * @see #gssKerberos5Service()
     */
    private void kerberos5ServiceCallBack(Callback[] callbacks) {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback n = (NameCallback) callback;
                n.setName("postgre/localhost");
            } else {
                throw new RuntimeException(MessageFormat.format("Not support {1}", callback.getClass().getName()));
            }
        }
    }


    private static boolean loadLoginFile() {
        URL url = Kerberos5Tests.class.getClassLoader().getResource("jaas/jaasTestLogin.config");
        boolean present = false;
        if (url != null) {
            final String key = "login.config.url.1", value = url.toString();
            Security.setProperty(key, value);
            Security.setProperty("java.security.krb5.kdc", "postgre.jdbd.io");
            Security.setProperty("java.security.krb5.realm", "POSTGRE.JDBD.IO");
            present = true;
        }
        return present;
    }


}
