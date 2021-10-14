package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
 */
final class Handshake10 implements MySQLPacket {

    public static Handshake10 readHandshake(ByteBuf payload) {
        // below payload
        if (payload.readByte() != 10) {
            throw new IllegalArgumentException("byteBuf isn't Handshake v10 .");
        }
        // 1. server version
        String serveVersionText = Packets.readStringTerm(payload, StandardCharsets.US_ASCII);
        MySQLServerVersion serverVersion = MySQLServerVersion.parseVersion(serveVersionText);
        // 2. thread id,a.k.a. connection id
        long threadId = Packets.readInt4AsLong(payload);
        // 3. auth-plugin-data-part-1,first 8 bytes of the plugin provided data (scramble)
        String authPluginDataPart1 = Packets.readStringFixed(payload, 8, StandardCharsets.US_ASCII);
        // 4. filler,0x00 byte, terminating the first part of a scramble
        short filler = (short) Packets.readInt1AsInt(payload);

        // 5. The lower 2 bytes of the Capabilities Flags
        int capabilityFlags = Packets.readInt2AsInt(payload);
        // 6. character_set,default server a_protocol_character_set, only the lower 8-bits
        short characterSet = (short) Packets.readInt1AsInt(payload);
        // 7. status_flags,SERVER_STATUS_flags_enum
        int statusFlags = Packets.readInt2AsInt(payload);
        // 8. read the upper 2 bytes of the Capabilities Flags and OR operation
        capabilityFlags |= (Packets.readInt2AsInt(payload) << 16);

        // 9. auth_plugin_data_len or skip.
        short authPluginDataLen;
        if ((capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            //length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
            authPluginDataLen = (short) Packets.readInt1AsInt(payload);
        } else {
            //skip constant 0x00
            payload.readByte();
            authPluginDataLen = 0;
        }
        // 10. reserved,reserved. All 0s.   skip for update read index
        payload.readerIndex(payload.readerIndex() + 10);

        // 11. auth-plugin-data-part-2,Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
        String authPluginDataPart2;
        authPluginDataPart2 = Packets.readStringFixed(payload
                , Integer.max(13, authPluginDataLen - 8), StandardCharsets.US_ASCII);
        // 12. auth_plugin_name,name of the auth_method that the auth_plugin_data belongs to
        String authPluginName = null;
        if ((capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            // Due to Bug#59453 the auth-plugin-name is missing the terminating NUL-char in versions prior to 5.5.10 and 5.6.2.
            if (!serverVersion.meetsMinimum(5, 5, 10)
                    || (serverVersion.meetsMinimum(5, 6, 0) && !serverVersion.meetsMinimum(5, 6, 2))) {
                authPluginName = Packets.readStringFixed(payload, authPluginDataLen, StandardCharsets.US_ASCII);
            } else {
                authPluginName = Packets.readStringTerm(payload, StandardCharsets.US_ASCII);
            }
        }
        return new Handshake10(serverVersion
                , threadId, authPluginDataPart1
                , filler, capabilityFlags
                , characterSet, statusFlags
                , authPluginDataLen, authPluginDataPart2
                , authPluginName);
    }


    private final MySQLServerVersion serverVersion;

    private final long threadId;


    private final short filler;

    private final int capabilityFlags;

    private final short collationIndex;

    private final int statusFlags;

    /**
     * optional
     */
    private final short authPluginDataLen;


    private final String pluginSeed;

    private final String authPluginName;

    private Handshake10(MySQLServerVersion serverVersion
            , long threadId, String authPluginDataPart1
            , short filler, int capabilityFlags
            , short collationIndex, int statusFlags
            , short authPluginDataLen, String authPluginDataPart2
            , @Nullable String authPluginName) {

        this.serverVersion = serverVersion;
        this.threadId = threadId;
        this.filler = filler;
        this.capabilityFlags = capabilityFlags;

        this.collationIndex = collationIndex;
        this.statusFlags = statusFlags;
        this.authPluginDataLen = authPluginDataLen;

        this.pluginSeed = authPluginDataPart1 + authPluginDataPart2;
        this.authPluginName = authPluginName;
    }

    public MySQLServerVersion getServerVersion() {
        return this.serverVersion;
    }

    public long getThreadId() {
        return this.threadId;
    }


    public String getPluginSeed() {
        return pluginSeed;
    }

    public short getFiller() {
        return this.filler;
    }

    public int getCapabilityFlags() {
        return this.capabilityFlags;
    }

    public short getCollationIndex() {
        return this.collationIndex;
    }

    public int getStatusFlags() {
        return this.statusFlags;
    }

    public short getAuthPluginDataLen() {
        return this.authPluginDataLen;
    }

    @Nullable
    public String getAuthPluginName() {
        return this.authPluginName;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HandshakeV10Packet{");
        sb.append("serverVersion=").append(serverVersion);
        sb.append(", threadId=").append(threadId);
        sb.append(", pluginSeed='").append(pluginSeed).append('\'');
        sb.append(", filler=").append(filler);
        sb.append(", capabilityFlags=").append(capabilityFlags);
        sb.append(", collationIndex=").append(collationIndex);
        sb.append(", statusFlags=").append(statusFlags);
        sb.append(", authPluginDataLen=").append(authPluginDataLen);
        sb.append(", authPluginName='").append(authPluginName).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
