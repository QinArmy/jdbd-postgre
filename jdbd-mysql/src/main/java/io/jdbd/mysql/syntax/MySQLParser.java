package io.jdbd.mysql.syntax;

import java.sql.SQLException;

public interface MySQLParser {

    MySQLStatement parse(String singleSql) throws SQLException;

    boolean isSingleStmt(String sql) throws SQLException;

    boolean isMultiStmt(String sql) throws SQLException;

}
