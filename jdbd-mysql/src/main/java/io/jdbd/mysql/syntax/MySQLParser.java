package io.jdbd.mysql.syntax;

import io.jdbd.vendor.syntax.SQLParser;

import java.sql.SQLException;

public interface MySQLParser extends SQLParser {

    @Override
    MySQLStatement parse(String singleSql) throws SQLException;

    boolean isSingleStmt(String sql) throws SQLException;

    boolean isMultiStmt(String sql) throws SQLException;

}
