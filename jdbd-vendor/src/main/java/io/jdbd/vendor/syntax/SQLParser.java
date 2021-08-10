package io.jdbd.vendor.syntax;


import java.sql.SQLException;

public interface SQLParser {


    SQLStatement parse(String singleSql) throws SQLException;


    boolean isSingleStmt(String sql) throws SQLException;

}
