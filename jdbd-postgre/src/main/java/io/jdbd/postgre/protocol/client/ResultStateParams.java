package io.jdbd.postgre.protocol.client;

final class ResultStateParams {

    int resultIndex;

    boolean moreResult;

    long affectedRows;

    long insertId;

    NoticeMessage noticeMessage;

    boolean hasReturningColumn;

    boolean moreFetch;


}
