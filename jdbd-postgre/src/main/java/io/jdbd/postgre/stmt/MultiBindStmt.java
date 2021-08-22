package io.jdbd.postgre.stmt;

import io.jdbd.vendor.stmt.StmtOptions;

import java.util.List;

public interface MultiBindStmt extends StmtOptions {

    List<BindableStmt> getStmtGroup();


}
