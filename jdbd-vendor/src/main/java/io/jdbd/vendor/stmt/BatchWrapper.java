package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchWrapper<T extends ParamValue> {

    String getSql();

    List<List<T>> getParamGroupList();

    int getTimeout();

}
