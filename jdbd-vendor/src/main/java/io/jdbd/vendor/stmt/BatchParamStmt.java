package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchParamStmt<T extends ParamValue> extends Stmt {


    List<List<T>> getGroupList();


}
