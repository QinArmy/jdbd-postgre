package io.jdbd.mysql.session;

import io.jdbd.PreparedStatement;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.SQLBindParameterException;
import io.jdbd.meta.SQLType;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;
import java.util.function.Consumer;

public final class ServerPreparedStatement extends AbstractPreparedStatement {


    public static ServerPreparedStatement create(PreparedStatementTask preparedTask) {
        return null;
    }

    private final PreparedStatementTask preparedTask;

    private ServerPreparedStatement(PreparedStatementTask preparedTask) {
        this.preparedTask = preparedTask;
    }

    @Override
    public Publisher<Integer> executeUpdate() {
        return null;
    }

    @Override
    public Publisher<Long> executeLargeUpdate() {
        return null;
    }

    @Override
    public Publisher<Integer> executeBatch() {
        return null;
    }

    @Override
    public Publisher<Long> executeLargeBatch() {
        return null;
    }

    @Override
    public Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public PreparedStatement bind(int index, JDBCType jdbcType, Object nonNullValue) throws SQLBindParameterException {
        return null;
    }

    @Override
    public PreparedStatement bind(int index, SQLType sqlType, Object nonNullValue) throws SQLBindParameterException {
        return null;
    }

    @Override
    public PreparedStatement bindNull(int index, JDBCType jdbcType) throws SQLBindParameterException {
        return null;
    }

    @Override
    public PreparedStatement bindNull(int index, SQLType sqlType) throws SQLBindParameterException {
        return null;
    }

    @Override
    public PreparedStatement addBatch() throws SQLBindParameterException {
        return null;
    }
}
