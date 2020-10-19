package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ServerVersion;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
 */
final class HandshakeV10Packet extends AbstractHandshakePacket {

    public static HandshakeV10Packet readHandshake(ByteBuf byteBuf) {
        // below packet header
        // 1. payload_length
        DataTypeUtils.readInt3(byteBuf);
        // 2. sequence_id
        DataTypeUtils.readInt1(byteBuf);
        // below payload
        if (DataTypeUtils.readInt1(byteBuf) != 10) {
            throw new IllegalArgumentException("byteBuf isn't Handshake v10 .");
        }
        // 1. server version
        String serveVersion = DataTypeUtils.readStringTerm(byteBuf, StandardCharsets.US_ASCII);
        // 2. thread id,a.k.a. connection id
        long threadId = DataTypeUtils.readInt4(byteBuf);
        // 3. auth-plugin-data-part-1,first 8 bytes of the plugin provided data (scramble)
        String authPluginDataPart1 = DataTypeUtils.readStringFixed(byteBuf, 8, StandardCharsets.US_ASCII);
        // 4. filler,0x00 byte, terminating the first part of a scramble
        short filler = DataTypeUtils.readInt1(byteBuf);

        // 5. The lower 2 bytes of the Capabilities Flags
        int capabilityFlags = DataTypeUtils.readInt2(byteBuf);
        // 6. character_set,default server a_protocol_character_set, only the lower 8-bits
        short characterSet = DataTypeUtils.readInt1(byteBuf);
        // 7. status_flags,SERVER_STATUS_flags_enum
        int statusFlags = DataTypeUtils.readInt2(byteBuf);
        // 8. read the upper 2 bytes of the Capabilities Flags and OR operation
        capabilityFlags |= (DataTypeUtils.readInt2(byteBuf) << 16);

        // 9. auth_plugin_data_len or skip.
        short authPluginDataLen;
        if ((capabilityFlags & ClientProtocol.CLIENT_PLUGIN_AUTH) != 0) {
            //length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
            authPluginDataLen = DataTypeUtils.readInt1(byteBuf);
        } else {
            //skip constant 0x00
            byteBuf.readByte();
            authPluginDataLen = 0;
        }
        // 10. reserved,reserved. All 0s.   skip for update read index
        byteBuf.readerIndex(byteBuf.readerIndex() + 10);

        // 11. auth-plugin-data-part-2,Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
        String authPluginDataPart2;
        authPluginDataPart2 = DataTypeUtils.readStringFixed(byteBuf
                , Integer.max(13, authPluginDataLen - 8), StandardCharsets.US_ASCII);
//        if (authPluginDataLen > 0) {
//            authPluginDataPart2 = DataTypeUtils.readStringFixed(byteBuf
//                    , authPluginDataLen - 8, StandardCharsets.US_ASCII);
//        } else {
//            authPluginDataPart2 = DataTypeUtils.readStringTerm(byteBuf,StandardCharsets.US_ASCII);
//        }
        // 12. auth_plugin_name,name of the auth_method that the auth_plugin_data belongs to
        String authPluginName = null;
        if ((capabilityFlags & ClientProtocol.CLIENT_PLUGIN_AUTH) != 0) {
            //    skip for update read index
            authPluginName = DataTypeUtils.readStringTerm(byteBuf, StandardCharsets.US_ASCII);
        }
        return new HandshakeV10Packet((short) 10, ServerVersion.parseVersion(serveVersion)
                , threadId, authPluginDataPart1
                , filler, capabilityFlags
                , characterSet, statusFlags
                , authPluginDataLen, authPluginDataPart2
                , authPluginName);
    }


    private final String authPluginDataPart1;

    private final short filler;

    private final int capabilityFlags;

    private final short characterSet;

    private final int statusFlags;

    /**
     * optional
     */
    private final short authPluginDataLen;

    private final String authPluginDataPart2;

    private final String authPluginName;

    private HandshakeV10Packet(short protocolVersion, ServerVersion serverVersion
            , long threadId, String authPluginDataPart1
            , short filler, int capabilityFlags
            , short characterSet, int statusFlags
            , short authPluginDataLen, String authPluginDataPart2
            , @Nullable String authPluginName) {

        super(protocolVersion, serverVersion, threadId);

        this.authPluginDataPart1 = authPluginDataPart1;
        this.filler = filler;
        this.capabilityFlags = capabilityFlags;
        this.characterSet = characterSet;

        this.statusFlags = statusFlags;
        this.authPluginDataLen = authPluginDataLen;
        this.authPluginDataPart2 = authPluginDataPart2;
        this.authPluginName = authPluginName;
    }

    public String getAuthPluginDataPart1() {
        return this.authPluginDataPart1;
    }

    public short getFiller() {
        return this.filler;
    }

    public int getCapabilityFlags() {
        return this.capabilityFlags;
    }

    public short getCharacterSet() {
        return this.characterSet;
    }

    public int getStatusFlags() {
        return this.statusFlags;
    }

    public short getAuthPluginDataLen() {
        return this.authPluginDataLen;
    }

    public String getAuthPluginDataPart2() {
        return this.authPluginDataPart2;
    }

    @Nullable
    public String getAuthPluginName() {
        return this.authPluginName;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", HandshakeV10Packet.class.getSimpleName() + "[", "]")
                .add("authPluginDataPart1='" + authPluginDataPart1 + "'")
                .add("filler=" + filler)
                .add("capabilityFlags=" + capabilityFlags)
                .add("characterSet=" + characterSet)
                .add("statusFlags=" + statusFlags)
                .add("authPluginDataLen=" + authPluginDataLen)
                .add("authPluginDataPart2='" + authPluginDataPart2 + "'")
                .add("authPluginName='" + authPluginName + "'")
                .toString();
    }
}
