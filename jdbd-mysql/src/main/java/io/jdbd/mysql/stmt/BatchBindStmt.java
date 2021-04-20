package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.BatchStmt;

import java.util.List;

public interface BatchBindStmt extends BatchStmt<BindValue> {

    @Override
    List<List<BindValue>> getGroupList();

}
