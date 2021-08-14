package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.BatchParamStmt;

import java.util.List;

public interface BatchBindStmt extends BatchParamStmt<BindValue> {

    @Override
    List<List<BindValue>> getGroupList();

}
