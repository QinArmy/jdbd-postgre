package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;

/**
 * @see EofPacket
 * @see OkPacket
 */
abstract class TerminatorPacket implements MySQLPacket {

    public static final int SERVER_STATUS_IN_TRANS = 1;
    public static final int SERVER_STATUS_AUTOCOMMIT = 2; // Server in auto_commit mode
    public static final int SERVER_MORE_RESULTS_EXISTS = 8; // Multi query - next query exists
    public static final int SERVER_QUERY_NO_GOOD_INDEX_USED = 1 << 4;
    public static final int SERVER_QUERY_NO_INDEX_USED = 1 << 5;
    public static final int SERVER_STATUS_CURSOR_EXISTS = 1 << 6;
    public static final int SERVER_STATUS_LAST_ROW_SENT = 1 << 7; // The server status for 'last-row-sent'
    public static final int SERVER_QUERY_WAS_SLOW = 1 << 11;
    public static final int SERVER_SESSION_STATE_CHANGED = 1 << 14;
    private final int warnings;

    final int statusFags;

    TerminatorPacket(int warnings, int statusFags) {
        this.warnings = warnings;
        this.statusFags = statusFags;
    }


    public final int getWarnings() {
        return this.warnings;
    }

    public final int getStatusFags() {
        return this.statusFags;
    }
}
