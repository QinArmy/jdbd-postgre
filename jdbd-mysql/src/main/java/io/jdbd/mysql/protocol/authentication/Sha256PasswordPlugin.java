package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.Properties;
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
import java.util.Objects;

/**
 * <p>
 * see {@code com.mysql.cj.protocol.a.authentication.Sha256PasswordPlugin}
 * </p>
 */
public class Sha256PasswordPlugin implements AuthenticationPlugin {


    public static Sha256PasswordPlugin getInstance(AuthenticateAssistant protocolAssistant) {

        return new Sha256PasswordPlugin(protocolAssistant, tryLoadPublicKeyString(protocolAssistant.getHostInfo()));

    }

    public static final String PLUGIN_NAME = "sha256_password";


    protected final AuthenticateAssistant protocolAssistant;

    protected final HostInfo<PropertyKey> hostInfo;

    protected final Properties<PropertyKey> env;

    protected boolean publicKeyRequested;

    protected String publicKeyString;

    protected String seed;


    protected Sha256PasswordPlugin(AuthenticateAssistant protocolAssistant
            , @Nullable String publicKeyString) {

        this.protocolAssistant = protocolAssistant;
        this.hostInfo = protocolAssistant.getHostInfo();
        this.publicKeyString = publicKeyString;
        this.env = protocolAssistant.getHostInfo().getProperties();
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

        final AuthenticateAssistant protocolAssistant = this.protocolAssistant;
        final String password = protocolAssistant.getHostInfo().getPassword();

        List<ByteBuf> toServer;
        if (MySQLStringUtils.isEmpty(password)
                || !fromServer.isReadable()) {
            ByteBuf byteBuf = protocolAssistant.allocator().buffer(1);
            byteBuf.writeZero(1);
            toServer = Collections.singletonList(byteBuf);
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
        ByteBuf payload;
        if (this.protocolAssistant.isUseSsl()) {
            // allow plain text over SSL
            payload = cratePlainTextPasswordPacket(password);
        } else if (this.publicKeyString != null) {
            // encrypt with given key, don't use "Public Key Retrieval"
            this.seed = PacketUtils.readStringTerm(fromServer, Charset.defaultCharset());
            payload = createEncryptPasswordPacketWithPublicKey(password);
        } else if (!this.env.getOrDefault(PropertyKey.allowPublicKeyRetrieval, Boolean.class)) {
            throw new MySQLJdbdException("Don't allow public key retrieval ,can't connect.");
        } else if (this.publicKeyRequested
                && fromServer.readableBytes() > ClientConstants.SEED_LENGTH) { // We must request the public key from the server to encrypt the password
            // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
            // so we check payload length to detect that.

            // read key response
            this.publicKeyString = PacketUtils.readStringTerm(fromServer, Charset.defaultCharset());
            payload = createEncryptPasswordPacketWithPublicKey(password);

            this.publicKeyRequested = false;
        } else {
            // build and send Public Key Retrieval packet
            this.seed = PacketUtils.readStringTerm(fromServer, Charset.defaultCharset());
            payload = createPublicKeyRetrievalPacket(getPublicKeyRetrievalPacketFlag());
            this.publicKeyRequested = true;
        }
        return payload.asReadOnly();
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
        byte[] seedBytes = Objects.requireNonNull(this.seed, "this.seed").getBytes();

        AuthenticateUtils.xorString(passwordBytes, mysqlScrambleBuff, seedBytes, passwordBytes.length);
        String publicKeyString = Objects.requireNonNull(this.publicKeyString, "this.publicKeyString");
        PublicKey publicKey = KeyUtils.readPublicKey(KeyPairType.RSA, publicKeyString);
        return encryptWithPublicKey(mysqlScrambleBuff, publicKey, transformation);
    }

    /**
     * @see #encryptPassword(String, String)
     */
    protected byte[] encryptWithPublicKey(byte[] mysqlScrambleBuff, PublicKey publicKey, String transformation) {
        try {
            Cipher cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.ENCRYPT_MODE, publicKey);
            return cipher.doFinal(mysqlScrambleBuff);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException
                | InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
            throw new MySQLJdbdException(e, "password encrypt error.");
        }
    }

    /**
     * @return read-only buffer
     */
    protected final ByteBuf cratePlainTextPasswordPacket(String password) {
        byte[] passwordBytes = password.getBytes(this.protocolAssistant.getHandshakeCharset());
        ByteBuf packetBuffer = this.protocolAssistant.allocator().buffer(passwordBytes.length + 1);
        PacketUtils.writeStringTerm(packetBuffer, passwordBytes);
        return packetBuffer.asReadOnly();
    }

    /**
     * @return read-only buffer
     */
    protected final ByteBuf createEncryptPasswordPacketWithPublicKey(String password) {
        byte[] passwordBytes = encryptPassword(password);

        ByteBuf packetBuffer = protocolAssistant.allocator().buffer(passwordBytes.length);
        packetBuffer.writeBytes(passwordBytes);
        return packetBuffer.asReadOnly();
    }

    /**
     * @param flag <ul>
     *             <li>1: sha256_password</li>
     *             <li>2: caching_sha2_password</li>
     *             </ul>
     */
    protected final ByteBuf createPublicKeyRetrievalPacket(int flag) {
        ByteBuf byteBuf = protocolAssistant.allocator().buffer(1);
        byteBuf.writeByte(flag);
        return byteBuf;
    }


    /*################################## blow static method ##################################*/

    @Nullable
    protected static String tryLoadPublicKeyString(HostInfo<PropertyKey> hostInfo) {
        String serverRSAPublicKeyPath = hostInfo.getProperties().getProperty(PropertyKey.serverRSAPublicKeyFile);
        String publicKeyString = null;
        try {
            if (serverRSAPublicKeyPath != null) {
                publicKeyString = readPathAsText(Paths.get(serverRSAPublicKeyPath));
            }
            return publicKeyString;
        } catch (Throwable e) {
            throw new MySQLJdbdException(e, "read serverRSAPublicKeyFile error.");
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
