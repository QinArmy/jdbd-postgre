package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

public final class AuthSwitchRequestPacket implements MySQLPacket {

    public static final int AUTH_SWITCH_REQUEST_HEADER = 0xFE;

    public static AuthSwitchRequestPacket readPacket(ByteBuf payloadBuf) {

        String pluginName = PacketUtils.readStringTerm(payloadBuf, StandardCharsets.US_ASCII);
        String pluginProviderData = PacketUtils.readStringTerm(payloadBuf, StandardCharsets.US_ASCII);

        return new AuthSwitchRequestPacket(pluginName, pluginProviderData);
    }

    private final String pluginName;

    private final String pluginProviderData;

    private AuthSwitchRequestPacket(String pluginName, String pluginProviderData) {
        this.pluginName = pluginName;
        this.pluginProviderData = pluginProviderData;
    }

    public String getPluginName() {
        return this.pluginName;
    }

    public String getPluginProviderData() {
        return this.pluginProviderData;
    }


    @Override
    public String toString() {
        return new StringJoiner(", ", AuthSwitchRequestPacket.class.getSimpleName() + "[", "]")
                .add("pluginName='" + pluginName + "'")
                .add("pluginProviderData='" + pluginProviderData + "'")
                .toString();
    }

}
