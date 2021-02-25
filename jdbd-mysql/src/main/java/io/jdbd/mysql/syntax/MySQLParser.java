package io.jdbd.mysql.syntax;

import io.jdbd.JdbdSQLException;

public interface MySQLParser {

    MySQLStatement parse(String singleSql) throws JdbdSQLException;

    boolean isSingleStmt(String sql);

    boolean isMultiStmt(String sql);

}
