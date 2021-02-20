package io.jdbd;

import io.jdbd.lang.NonNull;

public interface ResultStates {

    long getAffectedRows();

    long getInsertId();

    int getWarnings();

    @NonNull
    String getSQLState();

    int getVendorCode();

    boolean hasMoreResults();


}
