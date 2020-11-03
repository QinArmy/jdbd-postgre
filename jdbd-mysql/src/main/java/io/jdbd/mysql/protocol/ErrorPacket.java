package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.StringJoiner;

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
    public static ErrorPacket readPacket(ByteBuf payloadBuf) {
        if (PacketUtils.readInt1(payloadBuf) != ERROR_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        int errorCode = PacketUtils.readInt2(payloadBuf);

        String sqlStateMarker, sqlState, errorMessage;
        sqlStateMarker = PacketUtils.readStringFixed(payloadBuf, 1, Charset.defaultCharset());
        sqlState = PacketUtils.readStringFixed(payloadBuf, 5, Charset.defaultCharset());

        errorMessage = PacketUtils.readStringEof(payloadBuf, payloadBuf.readableBytes(), Charset.defaultCharset());

        return new ErrorPacket(errorCode, sqlStateMarker
                , sqlState, errorMessage
        );
    }

    public static ErrorPacket readPacketAtHandshake(ByteBuf packetBuf) {
        return readPacket(packetBuf);
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


    @Override
    public String toString() {
        return new StringJoiner(", ", ErrorPacket.class.getSimpleName() + "[", "]")
                .add("errorCode=" + errorCode)
                .add("sqlStateMarker='" + sqlStateMarker + "'")
                .add("sqlState='" + sqlState + "'")
                .add("errorMessage='" + errorMessage + "'")
                .toString();
    }

    public static boolean isErrorPacket(ByteBuf byteBuf) {
        return PacketUtils.getInt1(byteBuf, byteBuf.readerIndex()) == ERROR_HEADER;
    }

}



