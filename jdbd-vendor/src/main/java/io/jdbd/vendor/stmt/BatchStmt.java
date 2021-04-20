package io.jdbd.vendor.stmt;

import java.util.List;

public interface BatchStmt<T extends ParamValue> extends StmtWrapper {


    List<List<T>> getGroupList();


}
