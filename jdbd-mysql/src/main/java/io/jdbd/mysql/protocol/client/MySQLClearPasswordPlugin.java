package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.vendor.env.HostInfo;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class MySQLClearPasswordPlugin implements AuthenticationPlugin {

    public static MySQLClearPasswordPlugin getInstance(AuthenticateAssistant protocolAssistant) {
        return new MySQLClearPasswordPlugin(protocolAssistant);
    }

    public static final String PLUGIN_NAME = "mysql_clear_password";

    public static final String PLUGIN_CLASS = "com.mysql.cj.protocol.a.authentication.MysqlClearPasswordPlugin";

    private final AuthenticateAssistant protocolAssistant;

    private final HostInfo hostInfo;

    private MySQLClearPasswordPlugin(AuthenticateAssistant protocolAssistant) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = protocolAssistant.getHostInfo();
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

        AuthenticateAssistant protocolAssistant = this.protocolAssistant;
        Charset passwordCharset = protocolAssistant.getServerVersion().meetsMinimum(5, 7, 6)
                ? protocolAssistant.getPasswordCharset() : StandardCharsets.UTF_8;

        String password = this.hostInfo.getPassword();
        byte[] passwordBytes = password == null
                ? "".getBytes(passwordCharset)
                : password.getBytes(passwordCharset);

        ByteBuf payloadBuf = protocolAssistant.allocator().buffer(passwordBytes.length + 1);
        Packets.writeStringTerm(payloadBuf, passwordBytes);

        return Collections.singletonList(payloadBuf);
    }


}
