package io.jdbd.postgre.stmt;


import io.jdbd.vendor.stmt.ParamBatchStmt;

import java.util.List;

public interface BindBatchStmt extends ParamBatchStmt<BindValue> {

    @Override
    List<List<BindValue>> getGroupList();

}
