package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgServerVersion;

final class ResultStateParams {

    final PgServerVersion version;

    int resultIndex;

    boolean moreResult;

    long affectedRows;

    long insertId;

    long rowCount;

    NoticeMessage noticeMessage;

    boolean hasColumn;

    boolean moreFetch;

    ResultStateParams(PgServerVersion version) {
        this.version = version;
    }


}
