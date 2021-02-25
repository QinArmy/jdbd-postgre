package io.jdbd.vendor.statement;

import io.jdbd.JdbdSQLException;

public interface SQLParser {

    SQLStatement parse(String singleSql) throws JdbdSQLException;


}
