package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.BindValue;
import io.jdbd.vendor.IStatementWrapper;

import java.util.List;

public interface StatementWrapper extends IStatementWrapper<BindValue> {

    @Override
    List<List<BindValue>> getParameterGroupList();

    @Override
    List<BindValue> getParameterGroup();
}
