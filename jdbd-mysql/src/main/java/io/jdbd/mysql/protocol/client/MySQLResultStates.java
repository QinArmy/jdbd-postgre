package io.jdbd.mysql.protocol.client;

import io.jdbd.result.ResultStates;

abstract class MySQLResultStates implements ResultStates {

    static MySQLResultStates from(final int resultIndex, final TerminatorPacket terminator) {
        return new TerminalResultStates(resultIndex, terminator);
    }

    private final int resultIndex;

    private final int serverStatus;

    private final long affectedRows;

    private final long insertedId;

    private final String message;

    private final int warnings;


    private MySQLResultStates(final int resultIndex, final TerminatorPacket terminator) {
        this.resultIndex = resultIndex;
        if (terminator instanceof OkPacket) {
            OkPacket ok = (OkPacket) terminator;

            this.serverStatus = ok.getStatusFags();
            this.affectedRows = ok.getAffectedRows();
            this.insertedId = ok.getLastInsertId();
            this.message = ok.getInfo();

            this.warnings = ok.getWarnings();
        } else if (terminator instanceof EofPacket) {
            EofPacket eof = (EofPacket) terminator;

            this.serverStatus = eof.getStatusFags();
            this.affectedRows = 0L;
            this.insertedId = 0L;
            this.message = "";
            this.warnings = 0;
        } else {
            throw new IllegalArgumentException(String.format("terminator isn't %s or %s"
                    , OkPacket.class.getName(), EofPacket.class.getName()));
        }
    }


    @Override
    public final int getResultIndex() {
        return this.resultIndex;
    }

    @Override
    public final boolean supportInsertId() {
        return true;
    }


    @Override
    public final long getAffectedRows() {
        return this.affectedRows;
    }

    @Override
    public final long getInsertId() {
        return this.insertedId;
    }

    @Override
    public final String getMessage() {
        return this.message;
    }


    @Override
    public final boolean hasMoreResult() {
        return (this.serverStatus & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) != 0;
    }

    @Override
    public final boolean hasMoreFetch() {
        final int serverStatus = this.serverStatus;
        return (serverStatus & ClientProtocol.SERVER_STATUS_CURSOR_EXISTS) != 0
                && (serverStatus & ClientProtocol.SERVER_STATUS_LAST_ROW_SENT) == 0;
    }

    @Override
    public final int getWarnings() {
        return this.warnings;
    }

    private static final class TerminalResultStates extends MySQLResultStates {

        private TerminalResultStates(int resultIndex, TerminatorPacket terminator) {
            super(resultIndex, terminator);
        }

        @Override
        public final long getRowCount() {
            return 0L;
        }

        @Override
        public final boolean hasColumn() {
            return false;
        }

    }


}
