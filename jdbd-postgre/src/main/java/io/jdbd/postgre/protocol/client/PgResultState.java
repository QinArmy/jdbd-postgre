package io.jdbd.postgre.protocol.client;

import io.jdbd.result.ResultState;
import reactor.util.annotation.Nullable;

final class PgResultState implements ResultState {

    /** for EmptyQueryResponse */
    static final PgResultState EMPTY_STATE = new PgResultState(0L, 0L, null);

    static PgResultState create(String commandTag) {
        return null;
    }

    static PgResultState create(String commandTag, @Nullable NoticeMessage noticeMessage) {
        return null;
    }

    private final long affectedRows;

    private final long insertId;

    private final NoticeMessage noticeMessage;

    private PgResultState(long affectedRows, long insertId, @Nullable NoticeMessage noticeMessage) {
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
