package io.jdbd.vendor.stmt;

import io.jdbd.JdbdSQLException;

public interface SQLParser {

    SQLStatement parse(String singleSql) throws JdbdSQLException;


}
