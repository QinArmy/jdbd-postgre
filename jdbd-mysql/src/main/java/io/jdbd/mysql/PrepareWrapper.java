package io.jdbd.mysql;

import io.jdbd.vendor.statement.IPrepareWrapper;

import java.util.List;

public interface PrepareWrapper extends IPrepareWrapper<BindValue>, StmtWrapper {

    @Override
    List<List<BindValue>> getParameterGroupList();

    @Override
    List<BindValue> getParameterGroup();
}
