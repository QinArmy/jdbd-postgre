package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrBindStatement;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.function.Consumer;

/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.stmt.BindStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLBindStatement extends MySQLStatement implements AttrBindStatement {

    static MySQLBindStatement create(final MySQLDatabaseSession session, final String sql) {
        return new MySQLBindStatement(session, sql);
    }

    private final String sql;

    public MySQLBindStatement(final MySQLDatabaseSession session, final String sql) {
        super(session);
        this.sql = sql;
    }

    @Override
    public void bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException {

    }

    @Override
    public void bind(int indexBasedZero, SQLType sqlType, @Nullable Object nullable) throws JdbdException {

    }

    @Override
    public void bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException {

    }

    @Override
    public void bindCommonAttr(final String name, final MySQLType type, @Nullable final Object value) {

    }

    @Override
    public void bindAttr(final String name, final MySQLType type, @Nullable final Object value) {

    }


    @Override
    public void addBatch() throws JdbdException {

    }

    @Override
    public Publisher<ResultStates> executeUpdate() {
        return null;
    }

    @Override
    public Publisher<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public Publisher<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        return null;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        return null;
    }


}
