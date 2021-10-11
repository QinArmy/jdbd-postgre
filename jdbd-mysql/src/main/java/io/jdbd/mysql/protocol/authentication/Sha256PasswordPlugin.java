package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.client.Packets;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.util.JdbdStreams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.qinarmy.util.security.KeyPairType;
import org.qinarmy.util.security.KeyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.nio.charset.Charset;
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

    private static final Logger LOG = LoggerFactory.getLogger(Sha256PasswordPlugin.class);

    protected final AuthenticateAssistant protocolAssistant;

    protected final HostInfo<MyKey> hostInfo;

    protected final Properties<MyKey> env;

    private final String originalPublicKeyString;

    protected boolean publicKeyRequested;

    protected String publicKeyString;

    protected String seed;


    protected Sha256PasswordPlugin(AuthenticateAssistant protocolAssistant
            , @Nullable String publicKeyString) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = protocolAssistant.getHostInfo();
        this.originalPublicKeyString = publicKeyString;

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
    public void reset() {
        this.publicKeyString = this.originalPublicKeyString;
        this.publicKeyRequested = false;
        this.seed = null;
    }

    @Override
    public List<ByteBuf> nextAuthenticationStep(ByteBuf fromServer) {

        final AuthenticateAssistant protocolAssistant = this.protocolAssistant;
        final String password = protocolAssistant.getHostInfo().getPassword();

        List<ByteBuf> toServer;
        if (MySQLStrings.isEmpty(password)
                || !fromServer.isReadable()) {
            toServer = Collections.singletonList(Unpooled.EMPTY_BUFFER);
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
            LOG.trace("authenticate with server public key.");
            this.seed = Packets.readStringTerm(fromServer, Charset.defaultCharset());
            payload = createEncryptPasswordPacketWithPublicKey(password);
        } else if (!this.env.getOrDefault(MyKey.allowPublicKeyRetrieval, Boolean.class)) {
            throw new MySQLJdbdException("Don't allow public key retrieval ,can't connect.");
        } else if (this.publicKeyRequested
                && fromServer.readableBytes() > ClientConstants.SEED_LENGTH) { // We must request the public key from the server to encrypt the password
            // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
            // so we check payload length to detect that.
            LOG.trace("authenticate with request server public key.");
            // read key response
            this.publicKeyString = Packets.readStringTerm(fromServer, Charset.defaultCharset());
            payload = createEncryptPasswordPacketWithPublicKey(password);

            this.publicKeyRequested = false;
        } else {
            // build and send Public Key Retrieval packet
            this.seed = Packets.readStringTerm(fromServer, Charset.defaultCharset());
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
        passwordBytes = MySQLStrings.getBytesNullTerminated(password, this.protocolAssistant.getPasswordCharset());
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
        Packets.writeStringTerm(packetBuffer, passwordBytes);
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
    protected static String tryLoadPublicKeyString(HostInfo<MyKey> hostInfo) {

        try {
            String serverRSAPublicKeyPath = hostInfo.getProperties().get(MyKey.serverRSAPublicKeyFile);
            String publicKeyString = null;
            if (serverRSAPublicKeyPath != null) {
                publicKeyString = JdbdStreams.readAsString(Paths.get(serverRSAPublicKeyPath));
            }
            return publicKeyString;
        } catch (Throwable e) {
            throw new MySQLJdbdException(e, "read serverRSAPublicKeyFile error.");
        }
    }



}
