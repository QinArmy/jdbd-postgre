package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.Interval;
import io.jdbd.type.geometry.LongString;
import io.jdbd.vendor.result.AbstractResultRow;
import io.jdbd.vendor.type.LongStrings;
import org.qinarmy.util.Pair;
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
import java.time.OffsetTime;
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
    protected final <T> List<T> convertNonNullToList(final int indexBaseZero, final Object nonNull
            , final Class<T> elementClass)
            throws UnsupportedConvertingException {
        final PgColumnMeta meta = this.rowMeta.columnMetaArray[indexBaseZero];
        final List<T> value;
        if (meta.sqlType == PgType.TSVECTOR && elementClass == String.class) {
            value = convertTsvectorToList(meta, nonNull);
        } else if (!(nonNull instanceof byte[]) && nonNull.getClass().isArray() && meta.sqlType.isArray()) {
            final Pair<Class<?>, Integer> pair = PgBinds.getArrayDimensions(nonNull.getClass());
            final PgType elementType = Objects.requireNonNull(meta.sqlType.elementType());
            if (pair.getFirst() == elementType.javaType() && pair.getSecond() == 1) {
                value = convertOneDimensionArrayToList(nonNull, elementClass);
            } else {
                value = super.convertNonNullToList(indexBaseZero, nonNull, elementClass);
            }
        } else {
            value = super.convertNonNullToList(indexBaseZero, nonNull, elementClass);
        }
        return value;
    }

    @Override
    protected final Charset obtainColumnCharset(int indexBasedZero) {
        return this.rowMeta.clientCharset;
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
            case LINE:
            case BOX:
            case POLYGON: {
                need = columnClass != null && columnClass != String.class;
            }
            break;
            default: {
                if (meta.sqlType.isArray()) {
                    need = columnClass != null && columnClass != String.class;
                } else {
                    need = columnClass != String.class;
                }
            }
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
                value = parseArrayColumnFromText(textValue, meta, obtainArrayClass(meta, columnClass));
            } else {
                value = parseNonArrayColumnFromText(textValue, meta, columnClass);
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
                if (targetArrayClass != Boolean.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
                value = ColumnArrays.readBooleanArray(textValue, meta);
            }
            break;
            case SMALLINT_ARRAY: {
                if (targetArrayClass != Short.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
                value = ColumnArrays.readShortArray(textValue, meta);
            }
            break;
            case INTEGER_ARRAY: {
                if (targetArrayClass != Integer.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
                value = ColumnArrays.readIntegerArray(textValue, meta);
            }
            break;
            case OID_ARRAY:
            case BIGINT_ARRAY: {
                if (targetArrayClass != Long.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
                value = ColumnArrays.readBigIntArray(textValue, meta);
            }
            break;
            case DECIMAL_ARRAY: {
                value = ColumnArrays.readDecimalArray(textValue, meta, targetArrayClass);
            }
            break;
            case REAL_ARRAY: {
                if (targetArrayClass != Float.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
                value = ColumnArrays.readRealArray(textValue, meta);
            }
            break;
            case DOUBLE_ARRAY: {
                if (targetArrayClass != Double.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
                value = ColumnArrays.readDoubleArray(textValue, meta);
            }
            break;
            case TIME_ARRAY: {
                if (targetArrayClass != LocalTime.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
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
                if (targetArrayClass != OffsetTime.class) {
                    throw createNotSupportedException(meta.index, targetArrayClass);
                }
                value = ColumnArrays.readTimeTzArray(textValue, meta);
            }
            break;
            case TIMESTAMPTZ_ARRAY: {
                value = ColumnArrays.readTimestampTzArray(textValue, meta, targetArrayClass);
            }
            break;
            case VARBIT_ARRAY:
            case BIT_ARRAY: {
                value = ColumnArrays.readBitArray(textValue, meta, targetArrayClass);
            }
            break;
            case MONEY_ARRAY: {
                final DecimalFormat format = this.rowMeta.moneyFormat;
                if (format == null) {
                    throw moneyCannotConvertException(meta);
                }
                value = ColumnArrays.readMoneyArray(textValue, meta, targetArrayClass, format);
            }
            break;
            case UUID_ARRAY: {
                value = ColumnArrays.readUuidArray(textValue, meta, targetArrayClass);
            }
            break;
            case BYTEA_ARRAY: {
                value = ColumnArrays.readByteaArray(textValue, meta, this.rowMeta.clientCharset, targetArrayClass);
            }
            break;
            case TSQUERY_ARRAY:
            case TSVECTOR_ARRAY:
            case JSONB_ARRAY:
            case JSON_ARRAY:
            case XML_ARRAY:
            case TEXT_ARRAY: {
                value = ColumnArrays.readTextArray(textValue, meta, targetArrayClass);
            }
            break;
            case INT4RANGE_ARRAY:
            case TSRANGE_ARRAY:
            case TSTZRANGE_ARRAY:
            case DATERANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case CIDR_ARRAY:
            case INET_ARRAY:
            case MACADDR_ARRAY:
            case MACADDR8_ARRAY:
            case CHAR_ARRAY:
            case VARCHAR_ARRAY: {
                if (targetArrayClass != String.class) {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
                value = ColumnArrays.readTextArray(textValue, meta, targetArrayClass);
            }
            break;
            case INTERVAL_ARRAY: {
                value = ColumnArrays.readIntervalArray(textValue, meta, targetArrayClass);
            }
            break;
            case POINT_ARRAY: {
                value = ColumnArrays.readPointArray(textValue, meta, targetArrayClass);
            }
            break;
            case LINE_ARRAY: {
                value = ColumnArrays.readLineArray(textValue, meta, targetArrayClass);
            }
            break;
            case LINE_SEGMENT_ARRAY: {
                value = ColumnArrays.readLineSegmentArray(textValue, meta, targetArrayClass);
            }
            break;
            case BOX_ARRAY: {
                value = ColumnArrays.readBoxArray(textValue, meta, targetArrayClass);
            }
            break;
            case PATH_ARRAY: {
                value = ColumnArrays.readPathArray(textValue, meta, targetArrayClass);
            }
            break;
            case POLYGON_ARRAY: {
                value = ColumnArrays.readPolygonArray(textValue, meta, targetArrayClass);
            }
            break;
            case CIRCLES_ARRAY: {
                value = ColumnArrays.readCirclesArray(textValue, meta, targetArrayClass);
            }
            break;
            case REF_CURSOR_ARRAY: {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("read array type {}", meta.sqlType);
                }
                value = textValue;
            }
            break;
            default:
                value = textValue;
        }
        return value;
    }

    private Object parseNonArrayColumnFromText(final String textValue, final PgColumnMeta meta
            , @Nullable final Class<?> columnClass)
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
            case CHAR:
            case VARCHAR:
            case MACADDR:
            case MACADDR8:
            case INET:
            case CIDR:
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
            case TEXT:
            case JSON:
            case JSONB:
            case XML:
            case TSVECTOR:
            case TSQUERY: {
                value = LongStrings.fromString(textValue);
            }
            break;
            case MONEY: {// money format dependent on locale,so can't(also don't need) convert.
                if (columnClass == BigDecimal.class) {
                    value = parseMoney(meta, textValue, this.rowMeta.moneyFormat);
                } else {
                    value = textValue;
                }
            }
            break;
            case POINT: {
                value = PgGeometries.point(textValue);
            }
            break;
            case LINE: {
                value = PgGeometries.line(textValue);
            }
            break;
            case LINE_SEGMENT: {
                value = PgGeometries.lineSegment(textValue);
            }
            break;
            case PATH: {
                value = PgGeometries.path(textValue);
            }
            break;
            case BOX: {
                value = PgGeometries.box(textValue);
            }
            break;
            case CIRCLES: {
                value = PgGeometries.circle(textValue);
            }
            break;
            case POLYGON: {
                value = PgGeometries.polygon(textValue);
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
     * @see #parseColumn(int, Object, Class)
     */
    private Class<?> obtainArrayClass(final PgColumnMeta meta, @Nullable final Class<?> columnClass) {
        final PgType elementType = Objects.requireNonNull(meta.sqlType.elementType());
        final Class<?> arrayClass;
        if (columnClass == null) {
            arrayClass = elementType.javaType();
        } else if (columnClass.isArray()) {
            final Pair<Class<?>, Integer> pair = PgBinds.getArrayDimensions(columnClass);
            final Class<?> clazz = pair.getFirst();
            if (clazz == byte.class) {
                if (pair.getSecond() < 2) {
                    throw createNotSupportedException(meta.index, columnClass);
                }
                arrayClass = byte[].class;
            } else {
                arrayClass = clazz;
            }
        } else {
            throw createNotSupportedException(meta.index, columnClass);
        }
        return arrayClass;
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
    private <T> List<T> convertTsvectorToList(final PgColumnMeta meta, final Object nonNull)
            throws UnsupportedConvertingException {
        final String v;
        if (nonNull instanceof String) {
            v = (String) nonNull;
        } else if (nonNull instanceof LongString) {
            final LongString s = (LongString) nonNull;
            if (s.isString()) {
                v = s.asString();
            } else {
                throw createNotSupportedException(meta.index, List.class);
            }
        } else {
            throw createNotSupportedException(meta.index, List.class);
        }
        return parseTsvectorResult(meta, v);
    }


    /**
     * @see #convertTsvectorToList(PgColumnMeta, Object)
     */
    private <T> List<T> parseTsvectorResult(final PgColumnMeta meta, final String lexemes)
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
                    throw errorTsvectorOutput(meta);
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
                throw errorTsvectorOutput(meta);
            }
        }

        @SuppressWarnings("unchecked") final List<T> resultList = (List<T>) list;
        return resultList;
    }

    /**
     * @see #parseNonArrayColumnFromText(String, PgColumnMeta, Class)
     */
    static BigDecimal parseMoney(final PgColumnMeta meta, final String nonNull, @Nullable final DecimalFormat format)
            throws UnsupportedConvertingException {
        if (format == null) {
            throw moneyCannotConvertException(meta);
        }
        try {
            final Number value;
            value = format.parse(nonNull);
            if (!(value instanceof BigDecimal)) {
                throw moneyCannotConvertException(meta);
            }
            return (BigDecimal) value;
        } catch (Throwable e) {
            final PgType pgType = meta.sqlType;
            final String columnLabel = meta.columnLabel;
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


    private UnsupportedConvertingException errorTsvectorOutput(final PgColumnMeta meta) {
        String m = String.format("Column[index:%s,label:%s] tsvector type output format error, can't convert to List<String>."
                , meta.index, meta.columnLabel);
        return new UnsupportedConvertingException(m, meta.sqlType, List.class);
    }


    static UnsupportedConvertingException notSupportConverting(PgColumnMeta meta, Class<?> targetClass) {
        String message = String.format("Not support convert from (index[%s] label[%s] and sql type[%s]) to %s.",
                meta.index, meta.columnLabel
                , meta.sqlType, targetClass.getName());

        return new UnsupportedConvertingException(message, meta.sqlType, targetClass);
    }

    static JdbdSQLException createResponseTextColumnValueError(@Nullable Throwable cause, PgColumnMeta meta
            , String textValue) {
        String m = String.format("Server response text value[%s] error for PgColumnMeta[%s].", textValue, meta);
        return new JdbdSQLException(new SQLException(m, cause));
    }

    static JdbdSQLException createResponseTextColumnValueError(PgColumnMeta meta, String textValue) {
        return createResponseTextColumnValueError(null, meta, textValue);
    }

    private static UnsupportedConvertingException moneyCannotConvertException(final PgColumnMeta meta) {
        String format;
        format = "Column[index:%s,label:%s] %s.getCurrencyInstance(Locale) method don't return %s instance,so can't convert postgre %s type to java type BigDecimal,jdbd-postgre need to upgrade.";
        String msg = String.format(format
                , meta.index
                , meta.columnLabel
                , NumberFormat.class.getName()
                , DecimalFormat.class.getName()
                , meta.sqlType);
        return new UnsupportedConvertingException(msg, meta.sqlType, BigDecimal.class);
    }


}
