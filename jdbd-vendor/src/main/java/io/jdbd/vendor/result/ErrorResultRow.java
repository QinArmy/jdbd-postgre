package io.jdbd.vendor.result;

import io.jdbd.JdbdSQLException;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.UnsupportedConvertingException;

import java.util.List;
import java.util.Set;

public final class ErrorResultRow implements ResultRow {

    public static final ErrorResultRow INSTANCE = new ErrorResultRow();

    private ErrorResultRow() {
    }

    @Override
    public ResultRowMeta getRowMeta() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(int indexBaseZero) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(int indexBaseZero, Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(String columnAlias) throws JdbdSQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(String alias, Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> getSet(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> getSet(String columnAlias, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> getList(int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> getList(String columnAlias, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getNonNull(int indexBaseZero)
            throws JdbdSQLException, NullPointerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getNonNull(int indexBaseZero, Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException, NullPointerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getNonNull(String columnAlias)
            throws JdbdSQLException, NullPointerException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getNonNull(String columnAlias, Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException, NullPointerException {
        throw new UnsupportedOperationException();
    }


}
