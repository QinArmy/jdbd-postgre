package io.jdbd.mysql.stmt;

import io.jdbd.vendor.stmt.PrepareStmt;

public interface MySQLPrepareStmt extends PrepareStmt, MySQLParamSingleStmt {

    @Override
    MySQLParamSingleStmt getStmt();

}
