package io.jdbd.postgre.syntax;

import io.jdbd.postgre.ServerParameter;
import io.jdbd.vendor.stmt.Stmt;
import io.jdbd.vendor.syntax.SQLParser;

import java.sql.SQLException;
import java.util.function.Function;

public interface PgParser extends SQLParser {

    PgStatement parse(String singleSql) throws SQLException;


    CopyIn parseCopyIn(Stmt stmt, int stmtIndex) throws SQLException;


    static PgParser create(Function<ServerParameter, String> paramFunction) {
        return DefaultPgParser.create(paramFunction);
    }

}
