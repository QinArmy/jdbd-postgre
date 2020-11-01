package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.security.KeyPairType;
import org.qinarmy.util.security.KeyUtils;
import reactor.util.annotation.Nullable;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>
 * see {@code com.mysql.cj.protocol.a.authentication.Sha256PasswordPlugin}
 * </p>
 */
public class Sha256PasswordPlugin implements AuthenticationPlugin {


    public static Sha256PasswordPlugin getInstance(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {

        return new Sha256PasswordPlugin(protocolAssistant, hostInfo, tryLoadPublicKeyString(hostInfo));

    }

    public static final String PLUGIN_NAME = "sha256_password";

    public static final String PLUGIN_CLASS = "com.mysql.cj.protocol.a.authentication.Sha256PasswordPlugin";

    protected final ProtocolAssistant protocolAssistant;

    protected final HostInfo hostInfo;

    protected final AtomicReference<String> publicKeyString = new AtomicReference<>(null);

    protected final Properties env;

    private final AtomicBoolean hasServerRSAPublicKeyFile = new AtomicBoolean();

    protected final AtomicBoolean publicKeyRequested = new AtomicBoolean(false);

    protected final AtomicReference<String> seed = new AtomicReference<>(null);


    protected Sha256PasswordPlugin(ProtocolAssistant protocolAssistant, HostInfo hostInfo, @Nullable String publicKeyString) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = hostInfo;
        if (publicKeyString != null) {
            this.publicKeyString.set(publicKeyString);
        }
        this.hasServerRSAPublicKeyFile.set(publicKeyString != null);
        this.env = protocolAssistant.getMainHostInfo().getProperties();
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public List<ByteBuf> nextAuthenticationStep(ByteBuf fromServer) {

        final ProtocolAssistant protocolAssistant = this.protocolAssistant;
        final String password = protocolAssistant.getMainHostInfo().getPassword();

        List<ByteBuf> toServer;
        if (MySQLStringUtils.isEmpty(password)
                || !fromServer.isReadable()) {
            toServer = Collections.singletonList(protocolAssistant.createOneSizePayload(0));
        } else {
            ByteBuf payloadBuf = internalNextAuthenticationStep(password, fromServer);
            toServer = payloadBuf == null ? Collections.emptyList() : Collections.singletonList(payloadBuf);
        }
        return toServer;
    }

    @Nullable
    protected ByteBuf internalNextAuthenticationStep(String password, ByteBuf fromServer) {
        return doNextAuthenticationStep(password, fromServer);
    }


    protected final ByteBuf doNextAuthenticationStep(String password, ByteBuf fromServer) {
        ByteBuf payloadBuf;
        if (protocolAssistant.isUseSsl()) {
            // allow plain text over SSL
            payloadBuf = cratePlanTextPasswordPacket(password);
        } else if (this.hasServerRSAPublicKeyFile.get()) {
            // encrypt with given key, don't use "Public Key Retrieval"
            this.seed.set(PacketUtils.readStringTerm(fromServer, Charset.defaultCharset()));
            payloadBuf = createEncryptPasswordPacketWithPublicKey(password);
        } else if (!this.env.getRequiredProperty(PropertyKey.allowPublicKeyRetrieval, Boolean.class)) {
            throw new JdbdMySQLException("can't connect.");
        } else if (this.publicKeyRequested.get()
                && fromServer.readableBytes() > ClientConstants.SEED_LENGTH) { // We must request the public key from the server to encrypt the password
            // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
            // so we check payload length to detect that.

            // read key response
            this.publicKeyString.set(PacketUtils.readStringTerm(fromServer, Charset.defaultCharset()));
            payloadBuf = createEncryptPasswordPacketWithPublicKey(password);

            this.publicKeyRequested.set(false);
        } else {
            // build and send Public Key Retrieval packet
            this.seed.set(PacketUtils.readStringTerm(fromServer, Charset.defaultCharset()));
            payloadBuf = createPublicKeyRetrievalPacket(getPublicKeyRetrievalPacketFlag());
            this.publicKeyRequested.set(true);
        }
        return payloadBuf.asReadOnly();
    }

    protected int getPublicKeyRetrievalPacketFlag() {
        return 1;
    }

    protected byte[] encryptPassword(String password) {
        return encryptPassword(password, "RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
    }

    protected byte[] encryptPassword(String password, String transformation) {
        byte[] passwordBytes;
        passwordBytes = MySQLStringUtils.getBytesNullTerminated(password, this.protocolAssistant.getPasswordCharset());
        byte[] mysqlScrambleBuff = new byte[passwordBytes.length];
        AuthenticateUtils.xorString(passwordBytes, mysqlScrambleBuff, this.seed.get().getBytes(), passwordBytes.length);
        PublicKey publicKey = KeyUtils.readPublicKey(KeyPairType.RSA, this.publicKeyString.get());
        return encryptWithPublicKey(mysqlScrambleBuff, publicKey, transformation);
    }

    protected byte[] encryptWithPublicKey(byte[] mysqlScrambleBuff, PublicKey publicKey, String transformation) {
        try {
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return cipher.doFinal(mysqlScrambleBuff);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException
                | InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
            throw new JdbdMySQLException(e, "password encrypt error.");
        }
    }

    /**
     * @return read-only buffer
     */
    protected final ByteBuf cratePlanTextPasswordPacket(String password) {
        byte[] passwordBytes = password.getBytes(this.protocolAssistant.getClientCharset());
        ByteBuf packetBuffer = this.protocolAssistant.createPayloadBuffer(passwordBytes.length + 1);
        PacketUtils.writeStringTerm(packetBuffer, passwordBytes);
        PacketUtils.writeFinish(packetBuffer);
        return packetBuffer.asReadOnly();
    }

    /**
     * @return read-only buffer
     */
    protected final ByteBuf createEncryptPasswordPacketWithPublicKey(String password) {
        byte[] passwordBytes = encryptPassword(password);

        ByteBuf packetBuffer = protocolAssistant.createPayloadBuffer(passwordBytes.length);
        packetBuffer.writeBytes(passwordBytes);
        PacketUtils.writeFinish(packetBuffer);
        return packetBuffer.asReadOnly();
    }

    /**
     * @param flag <ul>
     *             <li>1: sha256_password</li>
     *             <li>2: caching_sha2_password</li>
     *             </ul>
     */
    protected final ByteBuf createPublicKeyRetrievalPacket(int flag) {
        return this.protocolAssistant.createOneSizePayload(flag);
    }


    /*################################## blow static method ##################################*/

    @Nullable
    protected static String tryLoadPublicKeyString(HostInfo hostInfo) {
        String serverRSAPublicKeyPath = hostInfo.getProperties()
                .getProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName());
        String publicKeyString = null;
        try {
            if (serverRSAPublicKeyPath != null) {
                publicKeyString = readPathAsText(Paths.get(serverRSAPublicKeyPath));
            }
            return publicKeyString;
        } catch (IOException e) {
            throw new JdbdMySQLException(e, "read serverRSAPublicKeyFile error.");
        }
    }


    private static String readPathAsText(Path path) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(path)) {

            Charset charset = Charset.forName(System.getProperty("file.encoding"));

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            StringBuilder builder = new StringBuilder();
            while (channel.read(buffer) > 0) {
                buffer.rewind();
                builder.append(charset.decode(buffer));
                buffer.flip();
            }
            return builder.toString();
        }
    }
}
