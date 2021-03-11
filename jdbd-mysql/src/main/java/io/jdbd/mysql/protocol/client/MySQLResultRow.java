package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.ResultRowMeta;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.vendor.result.AbstractResultRow;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

abstract class MySQLResultRow extends AbstractResultRow {

    static MySQLResultRow from(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        return new SimpleMySQLResultRow(columnValues, rowMeta, adjutant);
    }


    private final MySQLRowMeta rowMeta;

    private final ResultRowAdjutant adjutant;

    private MySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
        super(columnValues);
        if (columnValues.length != rowMeta.columnMetaArray.length) {
            throw new IllegalArgumentException(
                    String.format("columnValues length[%s] and columnMetas of rowMeta length[%s] not match."
                            , columnValues.length, rowMeta.columnMetaArray.length));
        }
        this.rowMeta = rowMeta;
        this.adjutant = adjutant;
    }

    @Override
    public ResultRowMeta obtainRowMeta() {
        return this.rowMeta;
    }

    /*################################## blow protected method ##################################*/

    @Override
    protected int convertToIndex(String columnAlias) {
        return this.rowMeta.convertToIndex(columnAlias);
    }

    @Override
    protected ZoneOffset obtainZoneOffsetClient() {
        return this.adjutant.obtainZoneOffsetClient();
    }

    @Override
    protected ZoneOffset obtainZoneOffsetDatabase() {
        return this.adjutant.obtainZoneOffsetDatabase();
    }

    @Override
    protected boolean isDatabaseSupportTimeZone() {
        return false;
    }

    @Override
    protected DateTimeFormatter obtainLocalDateTimeFormatter() {
        return MySQLTimeUtils.MYSQL_DATETIME_FORMATTER;
    }

    @Override
    protected DateTimeFormatter obtainLocalTimeFormatter() {
        return MySQLTimeUtils.MYSQL_TIME_FORMATTER;
    }

    @Override
    protected Charset obtainColumnCharset(final int index) {
        Charset charset = this.adjutant.getCharsetResults();
        if (charset == null) {
            charset = this.rowMeta.columnMetaArray[index].columnCharset;
        }
        return charset;
    }

    @Override
    protected JdbdSQLException createNotRequiredException(int indexBaseZero) {
        return new JdbdSQLException(new SQLException(
                String.format("Expected Object at index[%s] non-null,but null.", indexBaseZero)));
    }

    @Override
    protected JdbdSQLException createNotRequiredException(String alias) {
        return new JdbdSQLException(new SQLException(
                String.format("Expected Object at alias[%s] non-null,but null.", alias)));
    }

    @Override
    protected TemporalAccessor convertStringToTemporalAccessor(final int indexBaseZero, final String sourceValue
            , final Class<?> targetClass) {
        MySQLColumnMeta columnMeta = this.rowMeta.columnMetaArray[indexBaseZero];
        final TemporalAccessor accessor;
        try {
            switch (columnMeta.mysqlType) {
                case DATETIME:
                case TIMESTAMP:
                    accessor = convertStringToOffsetDateTime(indexBaseZero, sourceValue
                            , targetClass);
                    break;
                case DATE:
                    accessor = LocalDate.parse(sourceValue);
                    break;
                case TIME:
                    accessor = convertStringToOffsetTime(indexBaseZero, sourceValue, targetClass);
                    break;
                default:
                    throw createNotSupportedException(indexBaseZero, sourceValue.getClass(), targetClass);
            }
        } catch (DateTimeException e) {
            throw createValueCannotConvertException(e, indexBaseZero, sourceValue.getClass(), targetClass);
        }
        return accessor;
    }


    @Override
    protected JdbdSQLException createNotSupportedException(int indexBasedZero, Class<?> valueClass
            , Class<?> targetClass) {
        String m = String.format("Not support convert from (index[%s] alias[%s] and type[%s]) to %s."
                , indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , valueClass.getName(), targetClass.getName());
        return new JdbdSQLException(new SQLException(m));
    }

    @Override
    protected JdbdSQLException createValueCannotConvertException(@Nullable Throwable cause, int indexBasedZero
            , Class<?> valueClass, Class<?> targetClass) {
        String m = String.format("Cannot convert value from (index[%s] alias[%s] and type[%s]) to %s, please check value rang."
                , indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , valueClass.getName(), targetClass.getName());
        return new JdbdSQLException(new SQLException(m, cause));
    }



    /*################################## blow private method ##################################*/


    private static final class SimpleMySQLResultRow extends MySQLResultRow {

        private SimpleMySQLResultRow(Object[] columnValues, MySQLRowMeta rowMeta, ResultRowAdjutant adjutant) {
            super(columnValues, rowMeta, adjutant);
        }
    }
}
