package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html">Protocol::OK_Packet</a>
 */
public final class OkPacket implements MySQLPacket {

    public static final int OK_HEADER = 0;

    /**
     * @param payloadBuf a packet buffer than skip header .
     * @param capability <ul>
     *                   <li>before server receive handshake response packet: server capability</li>
     *                   <li>after server receive handshake response packet: negotiated capability</li>
     *                   </ul>
     * @throws IllegalArgumentException packet error.
     */
    public static OkPacket readPacket(ByteBuf payloadBuf, final int capability) {
        if (PacketUtils.readInt1(payloadBuf) != OK_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't ok packet.");
        }
        //1. affected_rows
        long affectedRows = PacketUtils.readLenEnc(payloadBuf);
        //2. last_insert_id
        long lastInsertId = PacketUtils.readLenEnc(payloadBuf);
        //3. status_flags and warnings
        final int statusFags, warnings;
        statusFags = PacketUtils.readInt2(payloadBuf);
        if ((capability & ClientCommandProtocol.CLIENT_PROTOCOL_41) != 0) {
            warnings = PacketUtils.readInt2(payloadBuf);
        } else {
            warnings = -1;
        }
        //4.
        String info, sessionStateInfo = null;
        if ((capability & ClientCommandProtocol.CLIENT_SESSION_TRACK) != 0) {
            info = PacketUtils.readStringLenEnc(payloadBuf, Charset.defaultCharset());
            if (info == null) {
                info = "";
            }
            if ((statusFags & ClientCommandProtocol.SERVER_SESSION_STATE_CHANGED) != 0) {
                sessionStateInfo = PacketUtils.readStringLenEnc(payloadBuf, Charset.defaultCharset());
            }
        } else {
            info = PacketUtils.readStringEof(payloadBuf, Charset.defaultCharset());
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
            , String info, @Nullable String sessionStateInfo) {

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

    public String getInfo() {
        return this.info;
    }

    @Nullable
    public String getSessionStateInfo() {
        return this.sessionStateInfo;
    }

    public static boolean isOkPacket(ByteBuf payloadBuf) {
        return PacketUtils.getInt1(payloadBuf, payloadBuf.readerIndex()) == OK_HEADER;
    }

}
