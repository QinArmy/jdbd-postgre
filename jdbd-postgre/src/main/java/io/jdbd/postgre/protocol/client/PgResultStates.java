package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.result.ResultState;
import reactor.util.annotation.Nullable;

import java.util.Set;
import java.util.StringTokenizer;

abstract class PgResultStates implements ResultState {

    static PgResultStates empty(int resultIndex, boolean moreResult) {
        return new EmptyResultState(resultIndex, moreResult);
    }

    static PgResultStates create(int resultIndex, boolean moreResult, String commandTag) {
        return create(resultIndex, moreResult, commandTag, null);
    }

    static PgResultStates create(int resultIndex, boolean moreResult, String commandTag
            , @Nullable NoticeMessage noticeMessage) {
        return new CommandResultState(resultIndex, moreResult, commandTag, noticeMessage);
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

        @Override
        public final boolean hasReturningColumn() {
            return false;
        }

    }


    private static final class CommandResultState extends PgResultStates {

        private final boolean moreResult;

        private final long affectedRows;

        private final long insertId;

        private final NoticeMessage noticeMessage;

        private final boolean hasReturningColumn;

        private CommandResultState(int resultIndex, boolean moreResult, String commandTag
                , @Nullable NoticeMessage noticeMessage) {
            super(resultIndex);
            this.moreResult = moreResult;
            this.noticeMessage = noticeMessage;

            final StringTokenizer tokenizer = new StringTokenizer(commandTag, " ");
            final String command;
            final long affectedRows, insertId;
            switch (tokenizer.countTokens()) {
                case 1:
                    command = tokenizer.nextToken();
                    affectedRows = insertId = 0L;
                    break;
                case 2:
                    command = tokenizer.nextToken();
                    affectedRows = Long.parseLong(tokenizer.nextToken());
                    insertId = 0L;
                    break;
                case 3:
                    command = tokenizer.nextToken();
                    insertId = Long.parseLong(tokenizer.nextToken());
                    affectedRows = Long.parseLong(tokenizer.nextToken());
                    break;
                default:
                    String m = String.format("Server response CommandComplete command tag[%s] format error."
                            , commandTag);
                    throw new PgJdbdException(m);
            }

            this.affectedRows = affectedRows;
            this.insertId = insertId;

            this.hasReturningColumn = QUERY_COMMAND.contains(command.toUpperCase());
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
            return false;
        }

        @Override
        public final boolean hasReturningColumn() {
            return this.hasReturningColumn;
        }

    }


}
