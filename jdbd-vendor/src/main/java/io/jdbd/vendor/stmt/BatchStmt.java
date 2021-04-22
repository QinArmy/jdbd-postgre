package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchStmt<T extends ParamValue> extends Stmt {


    List<List<T>> getGroupList();


}
