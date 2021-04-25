package io.jdbd.result;

public interface ResultState {

    long getAffectedRows();

    long getInsertId();

    int getWarnings();

    boolean hasMoreResult();

    boolean hasMoreFetch();


}
