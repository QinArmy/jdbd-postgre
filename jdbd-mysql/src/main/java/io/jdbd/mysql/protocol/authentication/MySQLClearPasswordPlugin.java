package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MySQLClearPasswordPlugin implements AuthenticationPlugin {

    public static MySQLClearPasswordPlugin getInstance(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {
        return new MySQLClearPasswordPlugin(protocolAssistant, hostInfo);
    }

    private final ProtocolAssistant protocolAssistant;

    private final HostInfo hostInfo;

    private MySQLClearPasswordPlugin(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = hostInfo;
    }

    @Override
    public String getProtocolPluginName() {
        return "mysql_clear_password";
    }

    @Override
    public boolean requiresConfidentiality() {
        return true;
    }

    @Override
    public boolean nextAuthenticationStep(@Nullable ByteBuf fromServer, List<ByteBuf> toServer) {
        toServer.clear();

        ProtocolAssistant protocolAssistant = this.protocolAssistant;
        Charset passwordCharset = protocolAssistant.getServerVersion().meetsMinimum(5, 7, 6)
                ? protocolAssistant.getPasswordCharset() : StandardCharsets.UTF_8;

        String password = this.hostInfo.getPassword();
        byte[] passwordBytes = password == null
                ? "".getBytes(passwordCharset)
                : password.getBytes(passwordCharset);

        ByteBuf packetBuffer = protocolAssistant.createPacketBuffer(passwordBytes.length + 1);
        PacketUtils.writeStringTerm(packetBuffer, passwordBytes);
        PacketUtils.writeFinish(packetBuffer);

        toServer.add(packetBuffer);
        return true;
    }


}
