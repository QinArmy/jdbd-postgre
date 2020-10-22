package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.security.DigestException;
import java.util.concurrent.atomic.AtomicReference;

public class CachingSha2PasswordPlugin extends Sha256PasswordPlugin {

    public static CachingSha2PasswordPlugin getInstance(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {
        return new CachingSha2PasswordPlugin(protocolAssistant, hostInfo, tryLoadPublicKeyString(hostInfo));
    }


    protected final AtomicReference<AuthStage> stage = new AtomicReference<>(AuthStage.FAST_AUTH_SEND_SCRAMBLE);

    protected CachingSha2PasswordPlugin(ProtocolAssistant protocolAssistant
            , HostInfo hostInfo, @Nullable String publicKeyString) {
        super(protocolAssistant, hostInfo, publicKeyString);
    }

    @Override
    public String getProtocolPluginName() {
        return "caching_sha2_password";
    }

    @Nullable
    @Override
    protected ByteBuf internalNextAuthenticationStep(String password, ByteBuf fromServer) {
        final AuthStage stage = this.stage.get();

        try {
            if (stage == AuthStage.FAST_AUTH_SEND_SCRAMBLE) {
                // send a scramble for fast auth
                String seedString = PacketUtils.readStringTerm(fromServer, Charset.defaultCharset());
                this.seed.set(seedString);

                byte[] passwordBytes = password.getBytes(protocolAssistant.getPasswordCharset());
                byte[] sha2Bytes = AuthenticateUtils.scrambleCachingSha2(passwordBytes, seedString.getBytes());
                ByteBuf packetBuffer = protocolAssistant.createPayloadBuffer(sha2Bytes.length);
                packetBuffer.writeBytes(sha2Bytes);
                return packetBuffer.asReadOnly();
            } else if (stage == AuthStage.FAST_AUTH_READ_RESULT) {
                switch (fromServer.readByte()) {
                    case 3:
                        this.stage.set(AuthStage.FAST_AUTH_COMPLETE);
                        return null;
                    case 4:
                        this.stage.set(AuthStage.FULL_AUTH);
                        break;
                    default:
                        throw new JdbdMySQLException("Unknown server response after fast auth.");
                }
            }
        } catch (DigestException e) {
            throw new JdbdMySQLException(e, "password encrypt failure.");
        }

        return doNextAuthenticationStep(password, fromServer);
    }

    @Override
    protected int getPublicKeyRetrievalPacketFlag() {
        return 2;
    }

    @Override
    protected byte[] encryptPassword(String password) {
        return this.protocolAssistant.getServerVersion().meetsMinimum(8, 0, 5)
                ? super.encryptPassword(password)
                : super.encryptPassword(password, "RSA/ECB/PKCS1Padding");
    }

    /*################################## blow static inner class ##################################*/


    public enum AuthStage {
        FAST_AUTH_SEND_SCRAMBLE,
        FAST_AUTH_READ_RESULT,
        FAST_AUTH_COMPLETE,
        FULL_AUTH;
    }

}
