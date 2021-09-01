package io.jdbd.postgre.session;

import io.jdbd.meta.SQLType;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.MultiStatement;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;

final class PgMultiStatement extends PgStatement implements MultiStatement {

    static PgMultiStatement create(PgDatabaseSession session) {
        return new PgMultiStatement(session);
    }

    private PgMultiStatement(PgDatabaseSession session) {
        super(session);
    }

    @Override
    public Publisher<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public void addStmt(String sql) {

    }

    @Override
    public void bind(int indexBasedZero, JDBCType jdbcType, Object nullable) {

    }

    @Override
    public void bind(int indexBasedZero, SQLType sqlType, Object nullable) {

    }

    @Override
    public void bind(int index, Object nullable) {

    }

    @Override
    public MultiResult executeBatchAsMulti() {
        return null;
    }

    @Override
    public Publisher<Result> executeBatchAsFlux() {
        return null;
    }
}
