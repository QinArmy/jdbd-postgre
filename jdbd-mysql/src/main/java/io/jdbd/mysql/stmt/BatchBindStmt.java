package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.ParamBatchStmt;

import java.util.List;

public interface BatchBindStmt extends ParamBatchStmt<BindValue> {

    @Override
    List<List<BindValue>> getGroupList();

}
