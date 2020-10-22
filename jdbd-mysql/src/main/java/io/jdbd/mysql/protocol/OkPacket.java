package io.jdbd.mysql.protocol;

import io.jdbd.mysql.protocol.client.PacketUtils;
import io.netty.buffer.ByteBuf;

public final class OkPacket implements MySQLPacket {

    public static OkPacket readPacket(ByteBuf byteBuf) {
        return null;
    }

    private final long affectedRows;

    private final long lastInsertId;

    private final int statusFags;

    private final int warnings;

    private final String info;

    private final String sessionStateInfo;

    private OkPacket(long affectedRows, long lastInsertId
            , int statusFags, int warnings
            , String info, String sessionStateInfo) {

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

    public String getSessionStateInfo() {
        return this.sessionStateInfo;
    }

    public static boolean isOkPacket(ByteBuf packetBuf) {
        return PacketUtils.getInt1(packetBuf, PacketUtils.HEADER_SIZE) == 0xFE;
    }

}
