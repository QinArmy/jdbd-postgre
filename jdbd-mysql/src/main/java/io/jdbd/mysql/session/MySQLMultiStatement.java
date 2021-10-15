package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrMultiStatement;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;

/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.stmt.MultiStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLMultiStatement extends MySQLStatement implements AttrMultiStatement {

    static MySQLMultiStatement create(MySQLDatabaseSession session) {
        return new MySQLMultiStatement(session);
    }

    private MySQLMultiStatement(MySQLDatabaseSession session) {
        super(session);
    }


    @Override
    public void addStatement(String sql) throws JdbdException {

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

    /*################################## blow private method ##################################*/


    @Override
    public boolean supportPublisher() {
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
    }


}
