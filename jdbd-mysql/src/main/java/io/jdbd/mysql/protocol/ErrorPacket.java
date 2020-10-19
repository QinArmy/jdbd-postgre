package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.DataTypeUtils;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html">Protocol::ERR_Packet</a>
 */
public final class ErrorPacket implements MySQLPacket {

    public static final short ERROR_PACKET_TYPE = 0xFF;

    /**
     * <p>
     * see {@code com.mysql.cj.protocol.a.NativeProtocol#rejectProtocol(com.mysql.cj.protocol.a.NativePacketPayload)}
     * </p>
     */
    public static ErrorPacket readPacket(ByteBuf byteBuf) {
        int payloadLength = DataTypeUtils.readInt3(byteBuf);

        if (DataTypeUtils.readInt1(byteBuf) != ERROR_PACKET_TYPE) {
            throw new IllegalArgumentException("byteBuf isn't error packet.");
        }
        return new ErrorPacket(DataTypeUtils.readInt2(byteBuf)
                , DataTypeUtils.readStringEof(byteBuf,payloadLength, StandardCharsets.US_ASCII));
    }

    private final int errorCode;

    private final String errorMessage;

    private ErrorPacket(int errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public int getErrorCode() {
        return this.errorCode;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public static boolean isErrorPacket(ByteBuf byteBuf) {
        return DataTypeUtils.getInt1(byteBuf, 4) == ERROR_PACKET_TYPE;
    }

}



