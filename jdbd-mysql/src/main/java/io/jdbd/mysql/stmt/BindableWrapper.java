package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.ParamWrapper;

import java.util.List;

public interface BindableWrapper extends ParamWrapper {

    /**
     * @return a unmodifiable list
     */
    @Override
    List<BindValue> getParamGroup();

}
