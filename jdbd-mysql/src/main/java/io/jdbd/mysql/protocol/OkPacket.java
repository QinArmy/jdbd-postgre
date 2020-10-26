package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html">Protocol::OK_Packet</a>
 */
public final class OkPacket implements MySQLPacket {

    public static final int OK_HEADER = 0;

    public static OkPacket readPacket(ByteBuf packetBuf, final int serverCapabilities) {
        final int payloadLength = PacketUtils.readInt3(packetBuf);
        // skip sequence_id
        packetBuf.readByte();
        if (PacketUtils.readInt1(packetBuf) != OK_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't ok packet.");
        }
        //1. affected_rows
        long affectedRows = PacketUtils.readLenEnc(packetBuf);
        //2. last_insert_id
        long lastInsertId = PacketUtils.readLenEnc(packetBuf);
        //3. status_flags and warnings
        final int statusFags, warnings;
        if ((serverCapabilities & ClientProtocol.CLIENT_PROTOCOL_41) != 0) {
            statusFags = PacketUtils.readInt2(packetBuf);
            warnings = PacketUtils.readInt2(packetBuf);
        } else {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        //4.
        String info, sessionStateInfo = null;
        if ((serverCapabilities & ClientProtocol.CLIENT_SESSION_TRACK) != 0) {
            info = PacketUtils.readStringLenEnc(packetBuf, Charset.defaultCharset());
            if ((statusFags & ClientProtocol.SERVER_SESSION_STATE_CHANGED) != 0) {
                sessionStateInfo = PacketUtils.readStringLenEnc(packetBuf, Charset.defaultCharset());
            }
        } else {
            info = PacketUtils.readStringEof(packetBuf, payloadLength, Charset.defaultCharset());
        }
        return new OkPacket(affectedRows, lastInsertId
                , statusFags, warnings, info, sessionStateInfo);
    }

    private final long affectedRows;

    private final long lastInsertId;

    private final int statusFags;

    private final int warnings;

    private final String info;

    private final String sessionStateInfo;

    private OkPacket(long affectedRows, long lastInsertId
            , int statusFags, int warnings
            , @Nullable String info, @Nullable String sessionStateInfo) {

        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.statusFags = statusFags;
        this.warnings = warnings;

        this.info = info;
        this.sessionStateInfo = sessionStateInfo;
    }

    public long getAffectedRows() {
        return this.affectedRows;
    }

    public long getLastInsertId() {
        return this.lastInsertId;
    }

    public int getStatusFags() {
        return this.statusFags;
    }

    public int getWarnings() {
        return this.warnings;
    }

    @Nullable
    public String getInfo() {
        return this.info;
    }

    @Nullable
    public String getSessionStateInfo() {
        return this.sessionStateInfo;
    }

    public static boolean isOkPacket(ByteBuf packetBuf) {
        return PacketUtils.getInt1(packetBuf, PacketUtils.HEADER_SIZE) == OK_HEADER;
    }

}
