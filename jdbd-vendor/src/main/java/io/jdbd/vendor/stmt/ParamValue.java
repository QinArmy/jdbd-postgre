package io.jdbd.vendor.stmt;

import io.jdbd.lang.Nullable;

public interface ParamValue {

    int getIndex();

    default boolean isLongData() {
        return false;
    }

    @Nullable
    Object get();

    Object getNonNull() throws NullPointerException;

}
