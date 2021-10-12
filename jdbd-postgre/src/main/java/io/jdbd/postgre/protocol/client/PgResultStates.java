package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgServerVersion;
import io.jdbd.result.ResultStates;

abstract class PgResultStates implements ResultStates {

    static PgResultStates empty(int resultIndex, boolean moreResult, PgServerVersion version) {
        return new EmptyResultStates(resultIndex, moreResult, version);
    }

    static PgResultStates create(ResultStateParams params) {
        return new CommandResultStates(params);
    }

    private final int resultIndex;

    private final boolean supportInsertId;

    private PgResultStates(final int resultIndex, PgServerVersion version) {
        if (resultIndex < 0) {
            throw new IllegalArgumentException(String.format("resultIndex[%s] less than 0 .", resultIndex));
        }
        this.resultIndex = resultIndex;
        this.supportInsertId = version.compareTo(PgServerVersion.V12) < 0;
    }

    @Override
    public final boolean supportInsertId() {
        return this.supportInsertId;
    }

    @Override
    public final int getResultIndex() {
        return this.resultIndex;
    }

    private static final class EmptyResultStates extends PgResultStates {

        private final boolean moreResult;

        private EmptyResultStates(int resultIndex, boolean moreResult, PgServerVersion version) {
            super(resultIndex, version);
            this.moreResult = moreResult;
        }


        @Override
        public final long getAffectedRows() {
            return 0L;
        }

        @Override
        public final long getInsertId() {
            return 0L;
        }

        @Override
        public final String getMessage() {
            return "";
        }

        @Override
        public final boolean hasMoreResult() {
            return this.moreResult;
        }

        @Override
        public final boolean hasMoreFetch() {
            return false;
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


    private static final class CommandResultStates extends PgResultStates {

        private final boolean moreResult;

        private final long affectedRows;

        private final long insertId;

        private final NoticeMessage noticeMessage;

        private final boolean hasReturningColumn;

        private final boolean moreFetch;

        private final long rowCount;

        private CommandResultStates(ResultStateParams params) {
            super(params.resultIndex, params.version);

            this.moreResult = params.moreResult;
            this.affectedRows = params.affectedRows;
            this.insertId = params.insertId;
            this.noticeMessage = params.noticeMessage;

            this.hasReturningColumn = params.hasColumn;
            this.moreFetch = params.moreFetch;
            this.rowCount = params.rowCount;
        }

        @Override
        public final long getAffectedRows() {
            return this.affectedRows;
        }

        @Override
        public final long getInsertId() {
            return this.insertId;
        }

        @Override
        public final String getMessage() {
            String message = null;
            NoticeMessage nm = this.noticeMessage;
            if (nm != null) {
                message = nm.getMessage();
            }
            if (message == null) {
                message = "";
            }
            return message;
        }

        @Override
        public final boolean hasMoreResult() {
            return this.moreResult;
        }

        @Override
        public final boolean hasMoreFetch() {
            return this.moreFetch;
        }

        @Override
        public final boolean hasColumn() {
            return this.hasReturningColumn;
        }


        @Override
        public final long getRowCount() {
            return this.rowCount;
        }

    }


}
