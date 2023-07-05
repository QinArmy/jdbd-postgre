package io.jdbd.postgre.stmt;


import io.jdbd.stmt.ParamStmt;

import java.util.List;

public interface BindStmt extends ParamStmt {

    @Override
    List<BindValue> getBindGroup();


}
