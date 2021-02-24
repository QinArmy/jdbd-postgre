package io.jdbd.mysql;

import io.jdbd.vendor.statement.IStmtWrapper;

import java.util.List;

public interface StmtWrapper extends IStmtWrapper<BindValue> {

    @Override
    List<BindValue> getParameterGroup();


}
