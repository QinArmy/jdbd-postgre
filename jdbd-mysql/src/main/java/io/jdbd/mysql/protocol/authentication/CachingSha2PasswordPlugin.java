package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.util.StringUtils;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.security.DigestException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class CachingSha2PasswordPlugin extends Sha256PasswordPlugin {


    protected final AtomicReference<AuthStage> stage = new AtomicReference<>(AuthStage.FAST_AUTH_SEND_SCRAMBLE);

    protected CachingSha2PasswordPlugin(ProtocolAssistant protocolAssistant, String publicKeyString) {
        super(protocolAssistant, publicKeyString);
    }

    @Override
    public String getProtocolPluginName() {
        return "caching_sha2_password";
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
            final AuthStage stage = this.stage.get();

            if (stage == AuthStage.FAST_AUTH_SEND_SCRAMBLE) {
                this.seed.set(PacketUtils.readStringTerm(fromServer, Charset.defaultCharset()));
                byte[] passwordBytes = password.getBytes(protocolAssistant.getPasswordCharset());
                byte[] sha2Bytes = AuthenticateUtils.scrambleCachingSha2(passwordBytes, this.seed.get().getBytes());

                ByteBuf packetBuffer = protocolAssistant.createPacketBuffer(sha2Bytes.length);
                PacketUtils.writeFinish(packetBuffer);

                toServer.add(packetBuffer);
                return true;
            } else if (stage == AuthStage.FAST_AUTH_READ_RESULT) {
                switch (fromServer.readByte()) {
                    case 3:
                        this.stage.set(AuthStage.FAST_AUTH_COMPLETE);
                        return true;
                    case 4:
                        this.stage.set(AuthStage.FULL_AUTH);
                        break;
                    default:
                        throw new JdbdMySQLException("Unknown server response after fast auth.");
                }
            }

            if (protocolAssistant.isUseSsl()) {
                // allow plain text over SSL
                toServer.add(cratePlanTextPasswordPacket(password));
            } else if (this.publicKeyString.get() != null) {

            }
        } catch (DigestException e) {
            throw new JdbdMySQLException(e, e.getMessage());
        }

        return true;
    }

    /*################################## blow static inner class ##################################*/


    public enum AuthStage {
        FAST_AUTH_SEND_SCRAMBLE,
        FAST_AUTH_READ_RESULT,
        FAST_AUTH_COMPLETE,
        FULL_AUTH;
    }

}
