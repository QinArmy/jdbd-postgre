package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchWrapper<T extends ParamValue> extends StmtWrapper {


    List<List<T>> getParamGroupList();


}
