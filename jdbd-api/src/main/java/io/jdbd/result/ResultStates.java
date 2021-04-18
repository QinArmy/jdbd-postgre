package io.jdbd.result;

public interface ResultStates {

    long getAffectedRows();

    long getInsertId();

    int getWarnings();

    boolean hasMoreResults();

    boolean hasMoreFetch();


}
