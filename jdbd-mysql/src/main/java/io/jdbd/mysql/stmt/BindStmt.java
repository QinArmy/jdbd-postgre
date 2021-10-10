package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.ParamStmt;

import java.util.List;

public interface BindStmt extends ParamStmt {

    /**
     * @return a unmodifiable list
     */
    @Override
    List<BindValue> getBindGroup();

}
