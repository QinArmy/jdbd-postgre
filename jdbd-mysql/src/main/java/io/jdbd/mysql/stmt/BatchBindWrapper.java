package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.BatchWrapper;

import java.util.List;

public interface BatchBindWrapper extends BatchWrapper<BindValue> {

    @Override
    List<List<BindValue>> getParamGroupList();

}
