package io.jdbd.vendor.stmt;

import java.util.List;

public interface ParamBatchStmt<T extends ParamValue> extends Stmt {

    String getSql();


    List<List<T>> getGroupList();


}
