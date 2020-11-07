package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
     *
     * @param capability <ul>
     *                   <li>before server receive handshake response packet: 0</li>
     *                   <li>after server receive handshake response packet: negotiated capability</li>
     *                   </ul>
     */
    public static ErrorPacket readPacket(ByteBuf payloadBuf, final int capability) {
        return readPacket(payloadBuf, capability, StandardCharsets.UTF_8);
    }

    public static ErrorPacket readPacketInHandshakePhase(ByteBuf payloadBuf) {
        return readPacket(payloadBuf, 0, StandardCharsets.UTF_8);
    }


    public static ErrorPacket readPacket(ByteBuf payloadBuf, final int capability, Charset errorMessageCharset) {
        if (PacketUtils.readInt1(payloadBuf) != ERROR_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        int errorCode = PacketUtils.readInt2(payloadBuf);

        String sqlStateMarker = null, sqlState = null, errorMessage;
        if ((capability & ClientProtocol.CLIENT_PROTOCOL_41) != 0) {
            sqlStateMarker = PacketUtils.readStringFixed(payloadBuf, 1, errorMessageCharset);
            sqlState = PacketUtils.readStringFixed(payloadBuf, 5, errorMessageCharset);
        }
        errorMessage = PacketUtils.readStringEof(payloadBuf, payloadBuf.readableBytes(), errorMessageCharset);

        return new ErrorPacket(errorCode, sqlStateMarker
                , sqlState, errorMessage
        );
    }


    private final int errorCode;

    private final String sqlStateMarker;

    private final String sqlState;

    private final String errorMessage;

    private ErrorPacket(int errorCode, @Nullable String sqlStateMarker, @Nullable String sqlState, String errorMessage) {
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

    @Nullable
    public String getSqlStateMarker() {
        return this.sqlStateMarker;
    }

    @Nullable
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



