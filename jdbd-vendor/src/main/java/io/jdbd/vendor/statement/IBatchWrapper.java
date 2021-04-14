package io.jdbd.vendor.statement;

import java.util.Collections;
import java.util.List;

public interface IBatchWrapper<T extends BindValue> extends IStmtWrapper<T> {


    List<List<T>> getParameterGroupList();

    /**
     * @return always {@link Collections#emptyList()}
     */
    @Override
    List<T> getParameterGroup();
}
