package io.jdbd.mysql.protocol.client;

import io.jdbd.ReactiveSQLException;
import io.jdbd.ResultRow;
import io.netty.buffer.ByteBuf;

abstract class MySQLResultRow implements ResultRow {

    static MySQLResultRow from(ByteBuf multiRowBuf) {
        return new SimpleMySQLMultiResultRow(multiRowBuf);
    }

    private final ByteBuf multiRowBuf;

    private MySQLResultRow(ByteBuf multiRowBuf) {
        this.multiRowBuf = multiRowBuf;
    }


    @Override
    public Object getObject(int indexBaseZero) throws ReactiveSQLException {
        return null;
    }

    @Override
    public <T> T getObject(int indexBaseZero, Class<T> columnClass) throws ReactiveSQLException {
        return null;
    }

    @Override
    public Object getObject(String alias) throws ReactiveSQLException {
        return null;
    }

    @Override
    public <T> T getObject(String alias, Class<T> columnClass) throws ReactiveSQLException {
        return null;
    }


    private static final class SimpleMySQLMultiResultRow extends MySQLResultRow {

        private SimpleMySQLMultiResultRow(ByteBuf multiRowBuf) {
            super(multiRowBuf);
        }
    }
}
