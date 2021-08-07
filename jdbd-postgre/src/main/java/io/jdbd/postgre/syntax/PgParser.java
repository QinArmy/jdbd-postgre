package io.jdbd.postgre.syntax;

import io.jdbd.vendor.syntax.SQLParser;

import java.sql.SQLException;

public interface PgParser extends SQLParser {

    PgStatement parse(String singleSql) throws SQLException;

}
