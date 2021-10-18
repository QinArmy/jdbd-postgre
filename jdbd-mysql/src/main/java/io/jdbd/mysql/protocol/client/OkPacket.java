package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html">Protocol::OK_Packet</a>
 */
public final class OkPacket extends TerminatorPacket {

    public static final int OK_HEADER = 0x00;

    /**
     * @param payload    a packet buffer than skip header .
     * @param capability <ul>
     *                   <li>before server receive handshake response packet: server capability</li>
     *                   <li>after server receive handshake response packet: negotiated capability</li>
     *                   </ul>
     * @throws IllegalArgumentException packet error.
     */
    public static OkPacket read(ByteBuf payload, final int capability) {
        int type = Packets.readInt1AsInt(payload);
        if (type != OK_HEADER && type != EofPacket.EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't ok packet.");
        }
        //1. affected_rows
        long affectedRows = Packets.readLenEnc(payload);
        //2. last_insert_id
        long lastInsertId = Packets.readLenEnc(payload);
        //3. status_flags and warnings
        final int statusFags, warnings;
        statusFags = Packets.readInt2AsInt(payload);
        if ((capability & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            warnings = Packets.readInt2AsInt(payload);
        } else {
            warnings = 0;
        }
        //4.
        String info = null, sessionStateInfo = null;
        if ((capability & Capabilities.CLIENT_SESSION_TRACK) != 0) {
            if (payload.isReadable()) {
                // here , avoid ResultSet terminator.
                info = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
            }
            if (info == null) {
                info = "";
            }
            if ((statusFags & TerminatorPacket.SERVER_SESSION_STATE_CHANGED) != 0) {
                sessionStateInfo = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
            }
        } else {
            info = Packets.readStringEof(payload, StandardCharsets.UTF_8);
        }
        return new OkPacket(affectedRows, lastInsertId
                , statusFags, warnings, info, sessionStateInfo);
    }


    private final long affectedRows;

    private final long lastInsertId;

    private final String info;

    private final String sessionStateInfo;

    private OkPacket(long affectedRows, long lastInsertId
            , int statusFags, int warnings
            , String info, @Nullable String sessionStateInfo) {
        super(warnings, statusFags);

        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.info = info;
        this.sessionStateInfo = sessionStateInfo;
    }

    public long getAffectedRows() {
        return this.affectedRows;
    }

    public long getLastInsertId() {
        return this.lastInsertId;
    }

    public String getInfo() {
        return this.info;
    }

    @Nullable
    public String getSessionStateInfo() {
        return this.sessionStateInfo;
    }


    public static boolean isOkPacket(ByteBuf payloadBuf) {
        return Packets.getInt1AsInt(payloadBuf, payloadBuf.readerIndex()) == OK_HEADER;
    }

    @Override
    public String toString() {
        return new StringBuilder("OkPacket{")
                .append("\naffectedRows=").append(affectedRows)
                .append("\n, lastInsertId=").append(lastInsertId)
                .append("\n, info='").append(info).append('\'')
                .append("\n, sessionStateInfo='").append(sessionStateInfo).append('\'')
                .append("\n, statusFags=").append(statusFags)
                .append("\n}").toString();
    }
}
