package io.jdbd.vendor.statement;

import io.jdbd.lang.Nullable;

public interface ParamValue {

    int getParamIndex();

    boolean isLongData();

    @Nullable
    Object getValue();

    Object getRequiredValue() throws NullPointerException;

}
