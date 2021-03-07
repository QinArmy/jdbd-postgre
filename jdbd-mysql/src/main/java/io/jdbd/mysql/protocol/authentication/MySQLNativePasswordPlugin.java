package io.jdbd.mysql.protocol.authentication;

import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.vendor.conf.HostInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Collections;
import java.util.List;

public class MySQLNativePasswordPlugin implements AuthenticationPlugin {


    public static MySQLNativePasswordPlugin getInstance(AuthenticateAssistant protocolAssistant) {
        return new MySQLNativePasswordPlugin(protocolAssistant);
    }

    public static final String PLUGIN_NAME = "mysql_native_password";

    public static final String PLUGIN_CLASS = "com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin";

    private final AuthenticateAssistant protocolAssistant;

    private final HostInfo<PropertyKey> hostInfo;

    private MySQLNativePasswordPlugin(AuthenticateAssistant protocolAssistant) {
        this.protocolAssistant = protocolAssistant;
        this.hostInfo = protocolAssistant.getHostInfo();
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
        String password = this.hostInfo.getPassword();

        ByteBuf payloadBuf;
        if (MySQLStringUtils.isEmpty(password)) {
            payloadBuf = Unpooled.EMPTY_BUFFER;
        } else {
            byte[] passwordBytes = password.getBytes(this.protocolAssistant.getPasswordCharset());
            byte[] seed = PacketUtils.readStringTermBytes(fromServer);
            byte[] scrambleBytes = AuthenticateUtils.scramble411(passwordBytes, seed);

            payloadBuf = this.protocolAssistant.allocator().buffer(scrambleBytes.length);
            payloadBuf.writeBytes(scrambleBytes);
        }
        return Collections.singletonList(payloadBuf);
    }


}
