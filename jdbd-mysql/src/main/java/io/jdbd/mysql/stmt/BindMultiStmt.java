package io.jdbd.mysql.stmt;


import io.jdbd.vendor.stmt.ParamMultiStmt;

import java.util.List;

@Deprecated
public interface BindMultiStmt extends ParamMultiStmt {

    List<BindStmt> getStmtList();
}
