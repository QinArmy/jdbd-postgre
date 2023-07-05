package io.jdbd.postgre.stmt;


import io.jdbd.vendor.stmt.ParamMultiStmt;

import java.util.List;

public interface BindMultiStmt extends ParamMultiStmt {

    List<BindStmt> getStmtList();


}
