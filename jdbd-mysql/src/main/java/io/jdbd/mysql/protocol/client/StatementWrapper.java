package io.jdbd.mysql.protocol.client;

import java.util.List;

public interface StatementWrapper extends io.jdbd.vendor.StatementWrapper<BindValue> {

    @Override
    List<List<BindValue>> getParameterGroupList();
}
