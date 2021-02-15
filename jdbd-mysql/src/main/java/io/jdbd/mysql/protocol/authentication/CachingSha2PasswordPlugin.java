package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.security.DigestException;
import java.util.concurrent.atomic.AtomicReference;

public class CachingSha2PasswordPlugin extends Sha256PasswordPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(CachingSha2PasswordPlugin.class);

    public static CachingSha2PasswordPlugin getInstance(AuthenticateAssistant protocolAssistant, HostInfo hostInfo) {
        return new CachingSha2PasswordPlugin(protocolAssistant, hostInfo, tryLoadPublicKeyString(hostInfo));
    }

    public static final String PLUGIN_NAME = "caching_sha2_password";

    public static final String PLUGIN_CLASS = "com.mysql.cj.protocol.a.authentication.CachingSha2PasswordPlugin";

    protected final AtomicReference<AuthStage> stage = new AtomicReference<>(AuthStage.FAST_AUTH_SEND_SCRAMBLE);

    protected CachingSha2PasswordPlugin(AuthenticateAssistant protocolAssistant
            , HostInfo hostInfo, @Nullable String publicKeyString) {
        super(protocolAssistant, hostInfo, publicKeyString);
    }

    @Override
    public void reset() {
        this.stage.set(AuthStage.FAST_AUTH_SEND_SCRAMBLE);
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
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

                byte[] passwordBytes = password.getBytes(this.protocolAssistant.getPasswordCharset());
                byte[] sha2Bytes = AuthenticateUtils.scrambleCachingSha2(passwordBytes, seedString.getBytes());
                ByteBuf payloadBuffer = this.protocolAssistant.createPayloadBuffer(sha2Bytes.length);
                payloadBuffer.writeBytes(sha2Bytes);

                this.stage.set(AuthStage.FAST_AUTH_READ_RESULT);
                return payloadBuffer.asReadOnly();
            } else if (stage == AuthStage.FAST_AUTH_READ_RESULT) {
                int flag = PacketUtils.readInt1(fromServer);
                switch (flag) {
                    case 3:
                        this.stage.set(AuthStage.FAST_AUTH_COMPLETE);
                        return null;
                    case 4:
                        this.stage.set(AuthStage.FULL_AUTH);
                        LOG.debug("Server demand FULL_AUTH");
                        break;
                    default:
                        throw new JdbdMySQLException("Unknown server response[%s] after fast auth.", flag);
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
