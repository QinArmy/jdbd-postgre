package io.jdbd.postgre.stmt;

import io.jdbd.vendor.stmt.ParamStmt;

import java.util.List;

public interface BindableStmt extends ParamStmt {

    default int getStmtIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    List<BindValue> getParamGroup();


}
