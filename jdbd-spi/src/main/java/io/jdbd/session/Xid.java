package io.jdbd.session;

import io.jdbd.lang.Nullable;

public interface Xid {

    String getGtrid();

    @Nullable
    String getBqual();

    int getFormatId();

}
