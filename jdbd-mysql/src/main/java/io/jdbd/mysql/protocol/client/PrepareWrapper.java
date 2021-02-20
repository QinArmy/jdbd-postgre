package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.BindValue;
import io.jdbd.vendor.statement.IPrepareWrapper;

import java.util.List;

public interface PrepareWrapper extends IPrepareWrapper<BindValue> {

    @Override
    List<List<BindValue>> getParameterGroupList();

    @Override
    List<BindValue> getParameterGroup();
}
