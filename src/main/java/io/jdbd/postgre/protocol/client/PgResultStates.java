package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgServerVersion;
import io.jdbd.result.ResultStates;
import io.jdbd.result.Warning;
import io.jdbd.session.Option;

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
    public final boolean isSupportInsertId() {
        return this.supportInsertId;
    }

    @Override
    public final int getResultNo() {
        return this.resultIndex;
    }

    private static final class EmptyResultStates extends PgResultStates {

        private final boolean moreResult;

        private EmptyResultStates(int resultIndex, boolean moreResult, PgServerVersion version) {
            super(resultIndex, version);
            this.moreResult = moreResult;
        }


        @Override
        public final long affectedRows() {
            return 0L;
        }

        @Override
        public final long lastInsertedId() {
            return 0L;
        }

        @Override
        public final String message() {
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
        public final long rowCount() {
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
        public  long affectedRows() {
            return this.affectedRows;
        }

        @Override
        public  long lastInsertedId() {
            return this.insertId;
        }

        @Override
        public  String message() {
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
        public  boolean hasMoreResult() {
            return this.moreResult;
        }

        @Override
        public  boolean hasMoreFetch() {
            return this.moreFetch;
        }

        @Override
        public  boolean hasColumn() {
            return this.hasReturningColumn;
        }


        @Override
        public  long rowCount() {
            return this.rowCount;
        }

        @Override
        public boolean inTransaction() {
            return false;
        }

        @Override
        public Warning warning() {
            return null;
        }

        @Override
        public <T> T valueOf(Option<T> option) {
            return null;
        }

    }


}
