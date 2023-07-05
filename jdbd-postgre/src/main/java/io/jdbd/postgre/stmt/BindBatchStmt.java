package io.jdbd.postgre.stmt;


import io.jdbd.stmt.ParamBatchStmt;

import java.util.List;

public interface BindBatchStmt extends ParamBatchStmt<BindValue> {

    @Override
    List<List<BindValue>> getGroupList();

}
