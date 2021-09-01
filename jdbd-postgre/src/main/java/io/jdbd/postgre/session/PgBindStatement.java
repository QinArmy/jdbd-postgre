package io.jdbd.postgre.session;

import io.jdbd.meta.SQLType;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.function.Consumer;

final class PgBindStatement extends PgStatement implements BindStatement {

    static PgBindStatement create(String sql, PgDatabaseSession session) {
        if (!PgStrings.hasText(sql)) {
            throw new IllegalArgumentException("sql must be have text.");
        }
        return new PgBindStatement(sql, session);
    }

    private final String sql;


    private PgBindStatement(String sql, PgDatabaseSession session) {
        super(session);
        this.sql = sql;
    }

    @Override
    public final void bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) {

    }

    @Override
    public final void bind(int indexBasedZero, SQLType sqlType, @Nullable Object nullable) {

    }

    @Override
    public final void bind(int indexBasedZero, @Nullable Object nullable) {

    }

    @Override
    public final void addBatch() {

    }

    @Override
    public final Publisher<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public final Publisher<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public final Publisher<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public final Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public final MultiResult executeBatchAsMulti() {
        return null;
    }

    @Override
    public final Publisher<Result> executeBatchAsFlux() {
        return null;
    }


}
