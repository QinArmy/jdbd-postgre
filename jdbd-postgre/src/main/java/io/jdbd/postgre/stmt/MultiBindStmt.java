package io.jdbd.postgre.stmt;

import io.jdbd.vendor.stmt.Stmt;

import java.util.List;

public interface MultiBindStmt extends Stmt {

    List<BindStmt> getStmtGroup();


}
