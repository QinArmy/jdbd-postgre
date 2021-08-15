package io.jdbd.postgre.protocol.client;

import io.jdbd.result.ResultState;
import reactor.util.annotation.Nullable;

abstract class PgResultStates implements ResultState {

    static PgResultStates empty(int resultIndex, boolean moreResult) {
        return new EmptyResultState(resultIndex, moreResult);
    }

    static PgResultStates create(int resultIndex, boolean moreResult, String commandTag) {
        return null;
    }

    static PgResultStates create(int resultIndex, boolean moreResult, String commandTag
            , @Nullable NoticeMessage noticeMessage) {
        return null;
    }


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

    private static final class EmptyResultState extends PgResultStates {

        private final boolean moreResult;

        private EmptyResultState(int resultIndex, boolean moreResult) {
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

    }


    private static final class CommandResultState extends PgResultStates {


        private final long affectedRows;

        private final long insertId;

        private final NoticeMessage noticeMessage;

        private CommandResultState(int resultIndex, long affectedRows, long insertId
                , @Nullable NoticeMessage noticeMessage) {
            super(resultIndex);
            this.affectedRows = affectedRows;
            this.insertId = insertId;
            this.noticeMessage = noticeMessage;
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
            return false;
        }

        @Override
        public final boolean hasMoreFetch() {
            return false;
        }

    }


}
