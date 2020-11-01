package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.protocol.ProtocolAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class MySQLClearPasswordPlugin implements AuthenticationPlugin {

    public static MySQLClearPasswordPlugin getInstance(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {
        return new MySQLClearPasswordPlugin(protocolAssistant, hostInfo);
    }

    public static final String PLUGIN_NAME = "mysql_clear_password";

    public static final String PLUGIN_CLASS = "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin";

    private final ProtocolAssistant protocolAssistant;

    private final HostInfo hostInfo;

    private MySQLClearPasswordPlugin(ProtocolAssistant protocolAssistant, HostInfo hostInfo) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = hostInfo;
    }

    @Override
    public String getProtocolPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return true;
    }

    @Override
    public List<ByteBuf> nextAuthenticationStep(ByteBuf fromServer) {

        ProtocolAssistant protocolAssistant = this.protocolAssistant;
        Charset passwordCharset = protocolAssistant.getServerVersion().meetsMinimum(5, 7, 6)
                ? protocolAssistant.getPasswordCharset() : StandardCharsets.UTF_8;

        String password = this.hostInfo.getPassword();
        byte[] passwordBytes = password == null
                ? "".getBytes(passwordCharset)
                : password.getBytes(passwordCharset);

        ByteBuf payloadBuf = protocolAssistant.createPayloadBuffer(passwordBytes.length + 1);
        PacketUtils.writeStringTerm(payloadBuf, passwordBytes);
        PacketUtils.writeFinish(payloadBuf);

        return Collections.singletonList(payloadBuf);
    }


}
