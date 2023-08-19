package io.jdbd.postgre.syntax;

import io.jdbd.JdbdException;
import io.jdbd.postgre.ServerParameter;
import io.jdbd.vendor.syntax.SQLParser;

import java.util.List;
import java.util.function.Function;

public interface PgParser extends SQLParser {

    PgStatement parse(String singleSql) throws JdbdException;

    List<String> separateMultiStmt(String multiStmt) throws JdbdException;

    CopyIn parseCopyIn(String sql) throws JdbdException;

    CopyOut parseCopyOut(String sql) throws JdbdException;

    String parseSetParameter(String sql) throws JdbdException;


    static PgParser create(Function<ServerParameter, String> paramFunction) {
        return DefaultPgParser.create(paramFunction);
    }

}
