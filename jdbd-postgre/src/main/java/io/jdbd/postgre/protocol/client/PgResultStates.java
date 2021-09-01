package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.util.PgArrays;
import io.jdbd.result.ResultStates;

import java.util.Set;

abstract class PgResultStates implements ResultStates {

    static PgResultStates empty(int resultIndex, boolean moreResult) {
        return new EmptyResultStates(resultIndex, moreResult);
    }

    static PgResultStates create(ResultStateParams params) {
        return new CommandResultStates(params);
    }

    private static final Set<String> QUERY_COMMAND = PgArrays.asUnmodifiableSet("SELECT", "SHOW");

    private final int resultIndex;

    private PgResultStates(final int resultIndex) {
        if (resultIndex < 0) {
            throw new IllegalArgumentException(String.format("resultIndex[%s] less than 0 .", resultIndex));
        }
        this.resultIndex = resultIndex;
    }

    @Override
    public final int getResultIndex() {
        return this.resultIndex;
    }

    private static final class EmptyResultStates extends PgResultStates {

        private final boolean moreResult;

        private EmptyResultStates(int resultIndex, boolean moreResult) {
            super(resultIndex);
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

        private CommandResultStates(ResultStateParams params) {
            super(params.resultIndex);

            this.moreResult = params.moreResult;
            this.affectedRows = params.affectedRows;
            this.insertId = params.insertId;
            this.noticeMessage = params.noticeMessage;

            this.hasReturningColumn = params.hasReturningColumn;
            this.moreFetch = params.moreFetch;
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

    }


}
