package io.jdbd.mysql.protocol;

public abstract class TerminatorPacket implements MySQLPacket {

    private final int warnings;

    private final int statusFags;

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
