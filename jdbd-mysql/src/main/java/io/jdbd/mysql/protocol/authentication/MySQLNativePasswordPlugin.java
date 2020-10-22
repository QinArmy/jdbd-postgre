package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.util.StringUtils;
import io.netty.buffer.ByteBuf;

import java.util.Collections;
import java.util.List;

public class MySQLNativePasswordPlugin implements AuthenticationPlugin {

    public static MySQLNativePasswordPlugin getInstance(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {
        return new MySQLNativePasswordPlugin(protocolAssistant, hostInfo);
    }

    private final ProtocolAssistant protocolAssistant;

    private final HostInfo hostInfo;

    private MySQLNativePasswordPlugin(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = hostInfo;
    }

    @Override
    public String getProtocolPluginName() {
        return "mysql_native_password";
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public List<ByteBuf> nextAuthenticationStep(ByteBuf fromServer) {
        String password = this.hostInfo.getPassword();

        ByteBuf payloadBuf;
        if (StringUtils.isEmpty(password)) {
            payloadBuf = this.protocolAssistant.createEmptyPayload();
        } else {
            byte[] passwordBytes = password.getBytes(this.protocolAssistant.getPasswordCharset());
            byte[] seed = PacketUtils.readStringTermBytes(fromServer);
            byte[] scrambleBytes = AuthenticateUtils.scramble411(passwordBytes, seed);

            payloadBuf = this.protocolAssistant.createPayloadBuffer(scrambleBytes.length);
            payloadBuf.writeBytes(scrambleBytes);
            PacketUtils.writeFinish(payloadBuf);
        }
        return Collections.singletonList(payloadBuf);
    }


}
