package io.jdbd.vendor;

import io.jdbd.JdbdSQLException;
import io.jdbd.NullMode;
import io.jdbd.ResultRowMeta;
import io.jdbd.meta.SQLType;

import java.sql.JDBCType;

public final class EmptyResultRowMeta implements ResultRowMeta {

    public static final EmptyResultRowMeta INSTANCE = new EmptyResultRowMeta();

    private EmptyResultRowMeta() {

    }

    @Override
    public int getColumnCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnLabel(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnIndex(String columnLabel) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public JDBCType getJdbdType(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPhysicalColumn(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLType getSQLType(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NullMode getNullMode(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSigned(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAutoIncrement(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCaseSensitive(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCatalogName(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSchemaName(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTableName(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getColumnName(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadOnly(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWritable(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<?> getColumnClass(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPrecision(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getScale(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPrimaryKey(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUniqueKey(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMultipleKey(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }


}
