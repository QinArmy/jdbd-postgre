package io.jdbd;

public interface ResultStates {

    long getAffectedRows();

    long getInsertId();

    int getWarnings();

    boolean hasMoreResults();

    boolean hasMoreFetch();


}
