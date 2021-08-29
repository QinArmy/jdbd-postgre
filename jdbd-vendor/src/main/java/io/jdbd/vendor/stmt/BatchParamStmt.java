package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchParamStmt<T extends ParamValue> extends Stmt {

    String getSql();


    List<List<T>> getGroupList();


}
