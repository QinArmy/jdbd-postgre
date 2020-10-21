package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.StringUtils;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Sha256PasswordPlugin implements AuthenticationPlugin {

    public static Sha256PasswordPlugin getInstance(ProtocolAssistant protocolAssistant) {
        String serverRSAPublicKeyPath = protocolAssistant.getMainHostInfo().getProperties()
                .getProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName());
        String publicKeyString = null;
        try {
            if (serverRSAPublicKeyPath != null) {
                publicKeyString = readPathAsText(Paths.get(serverRSAPublicKeyPath));
            }
        } catch (IOException e) {
            throw new JdbdMySQLException(e, "read serverRSAPublicKeyFile error.");
        }
        return new Sha256PasswordPlugin(protocolAssistant, publicKeyString);

    }


    protected final ProtocolAssistant protocolAssistant;

    protected final AtomicReference<String> publicKeyString = new AtomicReference<>(null);

    protected final Properties env;

    private final AtomicBoolean hasServerRSAPublicKeyFile = new AtomicBoolean();

    protected final AtomicBoolean publicKeyRequested = new AtomicBoolean(false);

    protected final AtomicReference<String> seed = new AtomicReference<>(null);


    protected Sha256PasswordPlugin(ProtocolAssistant protocolAssistant, @Nullable String publicKeyString) {
        this.protocolAssistant = protocolAssistant;
        if (publicKeyString != null) {
            this.publicKeyString.set(publicKeyString);
        }
        this.hasServerRSAPublicKeyFile.set(publicKeyString != null);
        this.env = protocolAssistant.getMainHostInfo().getProperties();
    }

    @Override
    public String getProtocolPluginName() {
        return "sha256_password";
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public boolean nextAuthenticationStep(@Nullable ByteBuf fromServer, List<ByteBuf> toServer) {
        toServer.clear();
        final ProtocolAssistant protocolAssistant = this.protocolAssistant;
        final String password = protocolAssistant.getMainHostInfo().getPassword();

        if (StringUtils.isEmpty(password)
                || fromServer == null
                || !fromServer.isReadable()) {
            toServer.add(protocolAssistant.createEmptyPacketForWrite());
            return true;
        }

        try {
            if (protocolAssistant.isUseSsl()) {
                // allow plain text over SSL
                toServer.add(cratePlanTextPasswordPacket(password));
            } else if (this.hasServerRSAPublicKeyFile.get()) {
                // encrypt with given key, don't use "Public Key Retrieval"
                this.seed.set(PacketUtils.readStringTerm(fromServer, Charset.defaultCharset()));
                toServer.add(createEncryptPasswordPacketWithPublicKey(password));
            } else if (!this.env.getRequiredProperty(PropertyKey.allowPublicKeyRetrieval, Boolean.class)) {
                throw new JdbdMySQLException("can't connect.");
            } else if (this.publicKeyRequested.get() && fromServer.readableBytes() > ClientConstants.SEED_LENGTH) { // We must request the public key from the server to encrypt the password
                // Servers affected by Bug#70865 could send Auth Switch instead of key after Public Key Retrieval,
                // so we check payload length to detect that.

                // read key response
                this.publicKeyString.set(PacketUtils.readStringTerm(fromServer, Charset.defaultCharset()));

                toServer.add(createEncryptPasswordPacketWithPublicKey(password));
                this.publicKeyRequested.set(false);
            } else {
                // build and send Public Key Retrieval packet
                this.seed.set(PacketUtils.readStringTerm(fromServer, Charset.defaultCharset()));

                ByteBuf packetBuffer = protocolAssistant.createPacketBuffer(1);
                PacketUtils.writeInt1(packetBuffer, 1);
                PacketUtils.writeFinish(packetBuffer);

                toServer.add(packetBuffer);

                this.publicKeyRequested.set(true);
            }
        } catch (JdbdMySQLException e) {
            throw new JdbdMySQLException(e, e.getMessage());
        }
        return true;
    }

    protected byte[] encryptPassword(@Nullable String password) {
        return encryptPassword(password, "RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
    }

    protected byte[] encryptPassword(@Nullable String password, String transformation) {
        byte[] passwordBytes;

        if (password == null) {
            passwordBytes = new byte[]{0};
        } else {
            passwordBytes = StringUtils.getBytesNullTerminated(password, this.protocolAssistant.getPasswordCharset());
        }
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
        ByteBuf packetBuffer = this.protocolAssistant.createPacketBuffer(passwordBytes.length + 1);
        PacketUtils.writeStringTerm(packetBuffer, passwordBytes);
        PacketUtils.writeFinish(packetBuffer);
        return packetBuffer.asReadOnly();
    }

    /**
     * @return read-only buffer
     */
    protected final ByteBuf createEncryptPasswordPacketWithPublicKey(String password) {
        byte[] passwordBytes = encryptPassword(password);

        ByteBuf packetBuffer = protocolAssistant.createPacketBuffer(passwordBytes.length);
        packetBuffer.writeBytes(passwordBytes);
        PacketUtils.writeFinish(packetBuffer);
        return packetBuffer.asReadOnly();
    }


    /*################################## blow static method ##################################*/


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
