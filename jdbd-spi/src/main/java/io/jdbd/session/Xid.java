package io.jdbd.session;

import io.jdbd.lang.Nullable;

public interface Xid {

    /**
     * <p>
     * The length that The global transaction identifier byte[]
     * </p>
     *
     * @return a global transaction identifier
     */
    String getGtrid();

    @Nullable
    String getBqual();

    int getFormatId();

}
