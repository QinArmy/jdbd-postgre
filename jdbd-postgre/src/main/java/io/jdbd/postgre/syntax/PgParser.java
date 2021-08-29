package io.jdbd.postgre.syntax;

import io.jdbd.postgre.ServerParameter;
import io.jdbd.vendor.syntax.SQLParser;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

public interface PgParser extends SQLParser {

    PgStatement parse(String singleSql) throws SQLException;

    List<String> separateMultiStmt(String multiStmt) throws SQLException;

    CopyIn parseCopyIn(String sql) throws SQLException;

    CopyOut parseCopyOut(String sql) throws SQLException;


    static PgParser create(Function<ServerParameter, String> paramFunction) {
        return DefaultPgParser.create(paramFunction);
    }

}
