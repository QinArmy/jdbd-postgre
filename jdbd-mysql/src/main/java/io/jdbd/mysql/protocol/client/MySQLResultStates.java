package io.jdbd.mysql.protocol.client;

import io.jdbd.result.ResultStates;

abstract class MySQLResultStates implements ResultStates {

    static MySQLResultStates fromUpdate(final int resultIndex, final TerminatorPacket terminator) {
        return new UpdateResultStates(resultIndex, terminator);
    }

    static MySQLResultStates fromQuery(final int resultIndex, final TerminatorPacket terminator, final long rowCount) {
        return new QueryResultStates(resultIndex, terminator, rowCount);
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
            final OkPacket ok = (OkPacket) terminator;
            this.serverStatus = ok.getStatusFags();
            this.affectedRows = ok.getAffectedRows();
            this.insertedId = ok.getLastInsertId();
            this.message = ok.getInfo();

            this.warnings = ok.getWarnings();
        } else if (terminator instanceof EofPacket) {
            final EofPacket eof = (EofPacket) terminator;

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

    public final boolean inTransaction() {
        final int serverStatus = this.serverStatus;
        return (serverStatus & TerminatorPacket.SERVER_STATUS_IN_TRANS) != 0
                || (serverStatus & TerminatorPacket.SERVER_STATUS_AUTOCOMMIT) != 0;
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
        return (this.serverStatus & TerminatorPacket.SERVER_MORE_RESULTS_EXISTS) != 0;
    }

    @Override
    public final boolean hasMoreFetch() {
        final int serverStatus = this.serverStatus;
        return (serverStatus & TerminatorPacket.SERVER_STATUS_CURSOR_EXISTS) != 0
                && (serverStatus & TerminatorPacket.SERVER_STATUS_LAST_ROW_SENT) == 0;
    }

    @Override
    public final int getWarnings() {
        return this.warnings;
    }

    private static final class UpdateResultStates extends MySQLResultStates {

        private UpdateResultStates(int resultIndex, TerminatorPacket terminator) {
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

    private static final class QueryResultStates extends MySQLResultStates {

        private final long rowCount;

        private QueryResultStates(int resultIndex, TerminatorPacket terminator, long rowCount) {
            super(resultIndex, terminator);
            this.rowCount = rowCount;
        }

        @Override
        public final long getRowCount() {
            return this.rowCount;
        }

        @Override
        public final boolean hasColumn() {
            return true;
        }

    }

}
