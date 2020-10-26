package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html">Protocol::ERR_Packet</a>
 */
public final class ErrorPacket implements MySQLPacket {

    public static final short ERROR_HEADER = 0xFF;

    /**
     * <p>
     * see {@code com.mysql.cj.protocol.a.NativeProtocol#rejectProtocol(com.mysql.cj.protocol.a.NativePacketPayload)}
     * </p>
     */
    public static ErrorPacket readPacket(ByteBuf packetBuf, final int serverCapabilities) {
        int payloadLength = PacketUtils.readInt3(packetBuf);
        // skip sequence_id
        packetBuf.readByte();
        if (PacketUtils.readInt1(packetBuf) != ERROR_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        int errorCode = PacketUtils.readInt2(packetBuf);
        String sqlStateMarker, sqlState;
        if ((serverCapabilities & ClientProtocol.CLIENT_PROTOCOL_41) != 0) {
            sqlStateMarker = PacketUtils.readStringFixed(packetBuf, 1, Charset.defaultCharset());
            sqlState = PacketUtils.readStringFixed(packetBuf, 5, Charset.defaultCharset());
        } else {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        String errorMessage = PacketUtils.readStringEof(packetBuf, payloadLength, Charset.defaultCharset());

        return new ErrorPacket(errorCode, sqlStateMarker
                , sqlState, errorMessage
        );
    }

    private final int errorCode;

    private final String sqlStateMarker;

    private final String sqlState;

    private final String errorMessage;

    private ErrorPacket(int errorCode, String sqlStateMarker, String sqlState, String errorMessage) {
        this.errorCode = errorCode;
        this.sqlStateMarker = sqlStateMarker;
        this.sqlState = sqlState;
        this.errorMessage = errorMessage;
    }

    public int getErrorCode() {
        return this.errorCode;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public String getSqlStateMarker() {
        return this.sqlStateMarker;
    }

    public String getSqlState() {
        return this.sqlState;
    }

    public static boolean isErrorPacket(ByteBuf byteBuf) {
        return PacketUtils.getInt1(byteBuf, PacketUtils.HEADER_SIZE) == ERROR_HEADER;
    }

}



