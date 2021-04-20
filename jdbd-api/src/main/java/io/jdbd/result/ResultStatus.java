package io.jdbd.result;

public interface ResultStatus {

    long getAffectedRows();

    long getInsertId();

    int getWarnings();

    boolean hasMoreResults();

    boolean hasMoreFetch();


}
