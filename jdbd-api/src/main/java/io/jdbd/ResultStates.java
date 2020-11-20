package io.jdbd;

import io.jdbd.lang.NonNull;

public interface ResultStates {

    long getAffectedRows();

    @NonNull
    Long getInsertId();

    int getWarnings();

    @NonNull
    String getSQLState();

    int getVendorCode();


}
