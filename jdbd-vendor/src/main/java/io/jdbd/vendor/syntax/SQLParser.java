package io.jdbd.vendor.syntax;


import io.jdbd.JdbdException;

public interface SQLParser {


    SQLStatement parse(String singleSql) throws JdbdException;


    boolean isSingleStmt(String sql) throws JdbdException;

}
