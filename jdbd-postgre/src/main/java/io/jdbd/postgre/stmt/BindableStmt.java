package io.jdbd.postgre.stmt;

import io.jdbd.vendor.stmt.ParamStmt;

import java.util.List;

public interface BindableStmt extends ParamStmt {

    @Override
    List<BindValue> getParamGroup();


}
