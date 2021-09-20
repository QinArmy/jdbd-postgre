package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.Interval;
import io.jdbd.type.geometry.LongString;
import io.jdbd.vendor.result.AbstractResultRow;
import io.jdbd.vendor.type.LongStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.DateTimeException;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.*;

public class PgResultRow extends AbstractResultRow<PgRowMeta> {

    static PgResultRow create(PgRowMeta rowMeta, Object[] columnValues, TaskAdjutant adjutant) {
        return new PgResultRow(rowMeta, columnValues, adjutant);
    }

    private static final Logger LOG = LoggerFactory.getLogger(PgResultRow.class);

    private final TaskAdjutant adjutant;


    private PgResultRow(PgRowMeta rowMeta, Object[] columnValues, TaskAdjutant adjutant) {
        super(rowMeta, columnValues);
        this.adjutant = adjutant;
    }


    @Override
    protected UnsupportedConvertingException createValueCannotConvertException(Throwable cause, int indexBasedZero, Class<?> targetClass) {
        return null;
    }

    @Override
    protected ZoneOffset obtainZoneOffsetClient() {
        return null;
    }

    @Override
    protected Charset obtainColumnCharset(int indexBasedZero) {
        return null;
    }

    @Override
    protected TemporalAccessor convertStringToTemporalAccessor(int indexBaseZero, String sourceValue, Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException {
        return null;
    }

    @Override
    protected TemporalAmount convertStringToTemporalAmount(int indexBaseZero, String sourceValue, Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException {
        return null;
    }

    @Override
    protected String formatTemporalAccessor(TemporalAccessor temporalAccessor) throws DateTimeException {
        return null;
    }

    @Override
    protected final boolean needParse(final int indexBaseZero, @Nullable final Class<?> columnClass) {
        final PgColumnMeta meta = this.rowMeta.obtainMeta(indexBaseZero);
        if (!meta.textFormat) {
            return false;
        }
        final boolean need;
        switch (meta.sqlType) {
            case BYTEA://This types that from text protocol have parsed by {@link io.jdbd.vendor.result.ResultSetReader}.
                need = false;
                break;
            case BOOLEAN:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMETZ:
            case TIMESTAMPTZ:
                need = true;
                break;
            default:
                need = columnClass != String.class;
        }
        return need;
    }

    @Override
    protected final Object parseColumn(final int indexBaseZero, final Object nonNull
            , @Nullable final Class<?> columnClass) {
        final PgColumnMeta meta = this.rowMeta.obtainMeta(indexBaseZero);
        final String textValue = (String) nonNull;
        try {
            final Object value;
            if (meta.sqlType.isArray()) {
                final PgType elementType = Objects.requireNonNull(meta.sqlType.elementType());
                final Class<?> arrayClass;
                if (columnClass == null) {
                    arrayClass = elementType.javaType();
                } else if (columnClass != byte[].class && columnClass.isArray()) {
                    arrayClass = PgBinds.getArrayDimensions(columnClass).getFirst();
                } else {
                    throw createNotSupportedException(indexBaseZero, columnClass);
                }
                value = parseArrayColumnFromText(textValue, meta, arrayClass);
            } else {
                value = parseNonArrayColumnFromText(textValue, meta);
            }
            return value;
        } catch (IllegalArgumentException | DateTimeException e) {
            throw createResponseTextColumnValueError(e, meta, textValue);
        }
    }


    private Object parseArrayColumnFromText(final String textValue, final PgColumnMeta meta
            , final Class<?> targetArrayClass)
            throws IllegalArgumentException {
        final Object value;
        switch (meta.sqlType) {
            case BOOLEAN_ARRAY: {
                value = ColumnArrays.readBooleanArray(textValue, meta);
            }
            break;
            case SMALLINT_ARRAY: {
                value = ColumnArrays.readShortArray(textValue, meta);
            }
            break;
            case INTEGER_ARRAY: {
                value = ColumnArrays.readIntegerArray(textValue, meta);
            }
            break;
            case BIGINT_ARRAY: {
                value = ColumnArrays.readBigIntArray(textValue, meta);
            }
            break;
            case DECIMAL_ARRAY: {
                value = ColumnArrays.readDecimalArray(textValue, meta);
            }
            break;
            case REAL_ARRAY: {
                value = ColumnArrays.readRealArray(textValue, meta);
            }
            break;
            case DOUBLE_ARRAY: {
                value = ColumnArrays.readDoubleArray(textValue, meta);
            }
            break;
            case TIME_ARRAY: {
                value = ColumnArrays.readTimeArray(textValue, meta);
            }
            break;
            case DATE_ARRAY: {
                value = ColumnArrays.readDateArray(textValue, meta, targetArrayClass);
            }
            break;
            case TIMESTAMP_ARRAY: {
                value = ColumnArrays.readTimestampArray(textValue, meta, targetArrayClass);
            }
            break;
            case TIMETZ_ARRAY: {
                value = ColumnArrays.readTimeTzArray(textValue, meta);
            }
            break;
            case TIMESTAMPTZ_ARRAY: {
                value = ColumnArrays.readTimestampTzArray(textValue, meta, targetArrayClass);
            }
            break;
            case MONEY_ARRAY:
            case TEXT_ARRAY:
            case BYTEA_ARRAY:
            case VARCHAR_ARRAY:
            case OID_ARRAY:
            case BIT_ARRAY:
            case INTERVAL_ARRAY:
            case CHAR_ARRAY:
            case VARBIT_ARRAY:
            case UUID_ARRAY:
            case XML_ARRAY:
            case POINT_ARRAY:
            case LINE_ARRAY:
            case LINE_SEGMENT_ARRAY:
            case JSONB_ARRAY:
            case JSON_ARRAY:
            case BOX_ARRAY:
            case PATH_ARRAY:
            case POLYGON_ARRAY:
            case CIRCLES_ARRAY:
            case CIDR_ARRAY:
            case INET_ARRAY:
            case MACADDR_ARRAY:
            case MACADDR8_ARRAY:
            case TSVECTOR_ARRAY:
            case REF_CURSOR_ARRAY:
            case INT4RANGE_ARRAY:
            case TSRANGE_ARRAY:
            case TSTZRANGE_ARRAY:
            case DATERANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case TSQUERY_ARRAY: {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("read array type {}", meta.sqlType);
                }
                value = textValue;
            }
            break;
            default:
                throw PgExceptions.createUnexpectedEnumException(meta.sqlType);
        }
        return value;
    }

    private Object parseNonArrayColumnFromText(final String textValue, final PgColumnMeta meta)
            throws IllegalArgumentException {
        final Object value;
        switch (meta.sqlType) {
            case SMALLINT: {
                value = Short.parseShort(textValue);
            }
            break;
            case INTEGER: {
                value = Integer.parseInt(textValue);
            }
            break;
            case OID:
            case BIGINT: {
                value = Long.parseLong(textValue);
            }
            break;
            case DECIMAL: {
                value = new BigDecimal(textValue);
            }
            break;
            case REAL: {
                value = Float.parseFloat(textValue);
            }
            break;
            case DOUBLE: {
                value = Double.parseDouble(textValue);
            }
            break;
            case TIMESTAMP: {
                value = PgTimes.parseIsoLocalDateTime(textValue);
            }
            break;
            case DATE: {
                value = PgTimes.parseIsoLocalDate(textValue);
            }
            break;
            case TIME: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = LocalTime.parse(textValue, PgTimes.ISO_LOCAL_TIME_FORMATTER);
            }
            break;
            case TIMESTAMPTZ: {
                value = PgTimes.parseIsoOffsetDateTime(textValue);
            }
            break;
            case TIMETZ: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = PgTimes.parseIsoOffsetTime(textValue);
            }
            break;
            case CHAR:
            case VARCHAR:
            case MONEY:// money format dependent on locale,so can't(also don't need) convert.
            case MACADDR:
            case MACADDR8:
            case INET:
            case CIDR:
            case LINE:
            case LINE_SEGMENT:
            case BOX:
            case INT4RANGE:
            case TSRANGE:
            case TSTZRANGE:
            case DATERANGE:
            case INT8RANGE: {
                value = textValue;
            }
            break;
            case VARBIT:
            case BIT: {
                value = PgStrings.bitStringToBitSet(textValue, false);
            }
            break;
            case UUID: {
                value = UUID.fromString(textValue);
            }
            break;
            case INTERVAL: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO-8601
                value = parseTemporalAmountFromText(textValue, meta);
            }
            break;
            case POINT: {
                value = PgGeometries.point(textValue);
            }
            break;
            case TEXT:
            case JSON:
            case JSONB:
            case XML:
            case POLYGON:
            case PATH:
            case TSVECTOR:
            case TSQUERY: {
                value = LongStrings.fromString(textValue);
            }
            break;
            case CIRCLES: {
                value = PgGeometries.circle(textValue);
            }
            break;
            case BOOLEAN: {
                if (textValue.equalsIgnoreCase("t")) {
                    value = Boolean.TRUE;
                } else if (textValue.equalsIgnoreCase("f")) {
                    value = Boolean.FALSE;
                } else {
                    throw PgResultRow.createResponseTextColumnValueError(meta, textValue);
                }
            }
            break;
            case BYTEA: {
                throw new IllegalArgumentException(String.format("%s have parsed.", meta.sqlType));
            }
            default: {
                // unknown type
                if (LOG.isTraceEnabled()) {
                    LOG.trace("read unknown type,meta:{}", meta);
                }
                value = textValue;
            }
        }
        return value;
    }

    /**
     * @see #getList(int, Class)
     */
    private <T> List<T> convertOneDimensionArrayToList(final Object array, final Class<T> elementClass) {
        final List<T> list;
        final int length = Array.getLength(array);
        switch (length) {
            case 0:
                list = Collections.emptyList();
                break;
            case 1:
                list = Collections.singletonList(elementClass.cast(Array.get(array, 0)));
                break;
            default: {
                final List<T> tempList = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    tempList.add(elementClass.cast(Array.get(array, i)));
                }
                list = Collections.unmodifiableList(tempList);
            }
        }
        return list;
    }


    private Interval parseTemporalAmountFromText(final String textValue, final PgColumnMeta meta) {
        final Interval value;
        switch (this.adjutant.server().intervalStyle()) {
            case iso_8601: {
                value = Interval.parse(textValue, true);
            }
            break;
            case postgres:
            case sql_standard:
            case postgres_verbose:
            default:
                throw new IllegalArgumentException(String.format("Cannot parse interval,ColumnMata[%s]", meta));
        }
        return value;
    }


    /**
     *
     */
    private <T> List<T> convertTsvectorToList(final int indexBaseZero, final Object nonNull)
            throws UnsupportedConvertingException {
        final String v;
        if (nonNull instanceof String) {
            v = (String) nonNull;
        } else if (nonNull instanceof LongString) {
            final LongString s = (LongString) nonNull;
            if (s.isString()) {
                v = s.asString();
            } else {
                throw createNotSupportedException(indexBaseZero, List.class);
            }
        } else {
            throw createNotSupportedException(indexBaseZero, List.class);
        }
        return parseTsvectorResult(indexBaseZero, v);
    }


    /**
     * @see #convertTsvectorToList(int, Object)
     */
    private <T> List<T> parseTsvectorResult(final int indexBaseZero, final String lexemes)
            throws UnsupportedConvertingException {
        final char[] charArray = lexemes.toCharArray();
        final int lastIndex = charArray.length - 1;
        final char QUOTE = '\'';
        boolean inQuoteString = false;
        char ch;
        final List<String> list = new ArrayList<>();
        for (int i = 0, lastEndpointEnd = 0; i < charArray.length; i++) {
            ch = charArray[i];
            if (inQuoteString) {
                final int index = lexemes.indexOf(QUOTE, i);
                if (index < 0) {
                    throw errorTsvectorOutput(indexBaseZero, lexemes);
                }
                if (index < lastIndex && charArray[index + 1] == QUOTE) {
                    // double quote Escapes
                    i = index + 1;
                } else {
                    i = index;
                    inQuoteString = false; // string constant end.
                    list.add(lexemes.substring(lastEndpointEnd, index));
                }
            } else if (ch == QUOTE) {
                inQuoteString = true;
                lastEndpointEnd = i + 1;
            } else if (!Character.isWhitespace(ch)) {
                throw errorTsvectorOutput(indexBaseZero, lexemes);
            }
        }

        @SuppressWarnings("unchecked") final List<T> resultList = (List<T>) list;
        return resultList;
    }


    /**
     *
     */
    private BigDecimal convertMoneyToBigDecimal(final int indexBaseZero, final String nonNull)
            throws UnsupportedConvertingException {
        final DecimalFormat format = this.rowMeta.moneyFormat;
        if (format == null) {
            throw moneyCannotConvertException(indexBaseZero);
        }
        try {
            final Number value;
            value = format.parse(nonNull);
            if (!(value instanceof BigDecimal)) {
                throw moneyCannotConvertException(indexBaseZero);
            }
            return (BigDecimal) value;
        } catch (Throwable e) {
            final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].sqlType;
            final String columnLabel = this.rowMeta.getColumnLabel(indexBaseZero);
            String m;
            m = String.format("Column[%s] postgre %s type convert to  java type BigDecimal failure."
                    , columnLabel
                    , pgType);
            if (e instanceof ParseException) {
                m = String.format("%s\nYou couldn't execute '%s' AND %s.get(\"%s\",%s.class) in multi-statement."
                        , m
                        , "SET lc_monetary"
                        , ResultRow.class.getName()
                        , columnLabel
                        , BigDecimal.class.getName()
                );
            }
            throw new UnsupportedConvertingException(m, e, pgType, BigDecimal.class);
        }
    }

    protected UnsupportedConvertingException createNotSupportedException(int indexBasedZero
            , Class<?> targetClass) {
        SQLType sqlType = this.rowMeta.getSQLType(indexBasedZero);

        String message = String.format("Not support convert from (index[%s] alias[%s] and MySQLType[%s]) to %s.",
                indexBasedZero, this.rowMeta.getColumnLabel(indexBasedZero)
                , sqlType, targetClass.getName());

        return new UnsupportedConvertingException(message, sqlType, targetClass);
    }

    private UnsupportedConvertingException errorTsvectorOutput(final int indexBaseZero, final String lexemes) {
        String m = String.format("[%s] is error tsvector type output, can't convert to List<String>.", lexemes);
        final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].sqlType;
        return new UnsupportedConvertingException(m, pgType, List.class);
    }

    private UnsupportedConvertingException moneyCannotConvertException(final int indexBasedZero) {
        PgType pgType = this.rowMeta.columnMetaArray[indexBasedZero].sqlType;
        String format;
        format = "%s.getCurrencyInstance(Locale) method don't return %s instance,so can't convert postgre %s type to java type BigDecimal,jdbd-postgre need to upgrade.";
        String msg = String.format(format
                , NumberFormat.class.getName()
                , DecimalFormat.class.getName()
                , pgType);
        return new UnsupportedConvertingException(msg, pgType, BigDecimal.class);
    }

    static JdbdSQLException createResponseTextColumnValueError(@Nullable Throwable cause, PgColumnMeta meta
            , String textValue) {
        String m = String.format("Server response text value[%s] error for PgColumnMeta[%s].", textValue, meta);
        return new JdbdSQLException(new SQLException(m, cause));
    }

    static JdbdSQLException createResponseTextColumnValueError(PgColumnMeta meta, String textValue) {
        return createResponseTextColumnValueError(null, meta, textValue);
    }


}
