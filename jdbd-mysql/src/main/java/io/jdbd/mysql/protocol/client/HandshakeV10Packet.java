package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.ServerVersion;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
 */
final class HandshakeV10Packet extends AbstractHandshakePacket {

    public static HandshakeV10Packet readHandshake(ByteBuf byteBuf) {

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
        //  skip for update read index
       // DataTypeUtils.readStringLenEnc(byteBuf, StandardCharsets.US_ASCII);
        // 12. auth_plugin_name,name of the auth_method that the auth_plugin_data belongs to
       // if ((capabilityFlags & ClientProtocol.CLIENT_PLUGIN_AUTH) != 0) {
            // skip for update read index
        //    DataTypeUtils.readStringTerm(byteBuf, StandardCharsets.US_ASCII);
       // }
        // skip 11 and 12 ,because mysql 	auth-plugin-data-part-2 has bug.
        byteBuf.readerIndex(byteBuf.writerIndex());
        return new HandshakeV10Packet((short)10, ServerVersion.parseVersion(serveVersion)
                , threadId, authPluginDataPart1
                , filler, capabilityFlags
                , characterSet, statusFlags
                , authPluginDataLen);
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

    private HandshakeV10Packet(short protocolVersion, ServerVersion serverVersion
            , long threadId, String authPluginDataPart1
            , short filler, int capabilityFlags
            , short characterSet, int statusFlags
            , short authPluginDataLen) {

        super(protocolVersion, serverVersion, threadId);

        this.authPluginDataPart1 = authPluginDataPart1;
        this.filler = filler;
        this.capabilityFlags = capabilityFlags;
        this.characterSet = characterSet;

        this.statusFlags = statusFlags;
        this.authPluginDataLen = authPluginDataLen;
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


}
