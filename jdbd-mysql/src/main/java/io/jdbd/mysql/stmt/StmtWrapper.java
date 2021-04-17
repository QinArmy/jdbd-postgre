package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.PrepareWrapper;

import java.util.List;

public interface StmtWrapper extends PrepareWrapper {

    /**
     * @return a unmodifiable list
     */
    @Override
    List<BindValue> getParamGroup();

}
