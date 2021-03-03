package io.jdbd.mysql;

import io.jdbd.vendor.statement.IBatchWrapper;

import java.util.Collections;
import java.util.List;

public interface BatchWrapper extends IBatchWrapper<BindValue>, StmtWrapper {

    @Override
    List<List<BindValue>> getParameterGroupList();

    /**
     * @return always {@link Collections#emptyList()}
     */
    @Override
    List<BindValue> getParameterGroup();
}
