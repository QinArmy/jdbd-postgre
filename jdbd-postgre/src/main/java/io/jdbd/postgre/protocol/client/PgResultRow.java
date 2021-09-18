package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgBox;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.type.PgLine;
import io.jdbd.postgre.type.PgPolygon;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.Interval;
import io.jdbd.type.geo.Line;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.LongString;
import io.jdbd.vendor.type.LongStrings;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdTimes;
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
import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;
import java.util.*;

public class PgResultRow implements ResultRow {

    static PgResultRow create(PgRowMeta rowMeta, Object[] columnValues, TaskAdjutant adjutant) {
        return new PgResultRow(rowMeta, columnValues, adjutant);
    }


    private static final Logger LOG = LoggerFactory.getLogger(PgResultRow.class);

    /**
     * This types that from text protocol have parsed by {@link io.jdbd.vendor.result.ResultSetReader}.
     */
    private static final Set<PgType> PARSED_TYPE_SET = Collections.unmodifiableSet(EnumSet.of(
            PgType.BYTEA,
            PgType.BOOLEAN
    ));


    private final PgRowMeta rowMeta;

    private final Object[] columnValues;

    private final TaskAdjutant adjutant;


    private PgResultRow(PgRowMeta rowMeta, Object[] columnValues, TaskAdjutant adjutant) {
        if (columnValues.length != rowMeta.columnMetaArray.length) {
            throw new IllegalArgumentException("columnValues length error.");
        }
        this.rowMeta = rowMeta;
        this.columnValues = columnValues;
        this.adjutant = adjutant;
    }

    @Override
    public final int getResultIndex() {
        return this.rowMeta.resultIndex;
    }

    @Override
    public final ResultRowMeta getRowMeta() {
        return this.rowMeta;
    }

    @Nullable
    @Override
    public final Object get(final int indexBaseZero) throws JdbdSQLException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return null;
        }
        final PgColumnMeta columnMeta = this.rowMeta.columnMetaArray[indexBaseZero];
        if (columnMeta.textFormat && !PARSED_TYPE_SET.contains(columnMeta.pgType)) {
            try {
                value = parseColumnFromText((String) value, columnMeta);
            } catch (IllegalArgumentException e) {
                throw createResponseTextColumnValueError(columnMeta, (String) value);
            }
        }
        value = convertNonNullValue(indexBaseZero, value, columnMeta.pgType.javaType());
        return value;
    }

    @Override
    public final Object get(final String columnLabel) throws JdbdSQLException {
        return get(this.rowMeta.getColumnIndex(columnLabel));
    }

    @Override
    public final <T> T get(final int indexBaseZero, final Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        Object value = this.columnValues[checkIndex(indexBaseZero)];
        if (value == null) {
            return null;
        }
        final PgColumnMeta columnMeta = this.rowMeta.columnMetaArray[indexBaseZero];
        if (columnMeta.textFormat && !PARSED_TYPE_SET.contains(columnMeta.pgType) && columnClass != String.class) {
            try {
                value = parseColumnFromText((String) value, columnMeta);
            } catch (IllegalArgumentException e) {
                throw createResponseTextColumnValueError(columnMeta, (String) value);
            }
        }
        return convertNonNullValue(indexBaseZero, value, columnClass);
    }


    @Override
    public final <T> T get(final String columnLabel, final Class<T> columnClass) {
        return get(this.rowMeta.getColumnIndex(columnLabel), columnClass);
    }

    @Override
    public final <T> Set<T> getSet(final int indexBaseZero, final Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        final PgColumnMeta columnMeta = this.rowMeta.columnMetaArray[checkIndex(indexBaseZero)];
        if (columnMeta.pgType.isArray()
                || columnMeta.pgType == PgType.BYTEA
                || elementClass != columnMeta.pgType.javaType()) {
            throw createNotSupportedException(indexBaseZero, Set.class);
        }
        final T value = get(indexBaseZero, elementClass);
        final Set<T> set;
        if (value == null) {
            set = Collections.emptySet();
        } else if (value instanceof String) {
            @SuppressWarnings("unchecked") final Set<T> temp = (Set<T>) PgStrings.spitAsSet((String) value, ",", true);
            set = temp;
        } else {
            set = Collections.singleton(value);
        }
        return set;
    }

    @Override
    public final <T> Set<T> getSet(final String columnLabel, final Class<T> elementClass) {
        return getSet(this.rowMeta.getColumnIndex(columnLabel), elementClass);
    }

    @Override
    public final <T> List<T> getList(final int indexBaseZero, Class<T> elementClass)
            throws JdbdSQLException, UnsupportedConvertingException {
        final PgColumnMeta columnMeta = this.rowMeta.columnMetaArray[checkIndex(indexBaseZero)];
        if (columnMeta.pgType == PgType.BYTEA) {
            throw createNotSupportedException(indexBaseZero, List.class);
        } else {
            final PgType elementType = columnMeta.pgType.elementType();
            if (elementType != null && elementClass != elementType.javaType()) {
                throw createNotSupportedException(indexBaseZero, List.class);
            }
        }

        final Object value = get(indexBaseZero);
        final List<T> list;
        if (value == null) {
            list = Collections.emptyList();
        } else if (value instanceof String) {
            @SuppressWarnings("unchecked") final List<T> temp = (List<T>) PgStrings.spitAsList((String) value, ","
                    , true);
            list = temp;
        } else if (value.getClass().isArray()) {
            final Pair<Class<?>, Integer> pair = PgBinds.getArrayDimensions(value.getClass());
            if (pair.getSecond() != 1 || elementClass != pair.getFirst()) {
                throw createNotSupportedException(indexBaseZero, List.class);
            }
            list = convertOneDimensionArrayToList(value, elementClass);
        } else if (elementClass.isInstance(value)) {
            list = Collections.singletonList(elementClass.cast(value));
        } else {
            throw createNotSupportedException(indexBaseZero, List.class);
        }
        return list;
    }


    @Override
    public final <T> List<T> getList(final String columnLabel, Class<T> elementClass) {
        return getList(this.rowMeta.getColumnIndex(columnLabel), elementClass);
    }

    @Override
    public final <K, V> Map<K, V> getMap(final int indexBaseZero, Class<K> keyClass, final Class<V> valueClass) {
        throw createNotSupportedException(indexBaseZero, Map.class);
    }

    @Override
    public final <K, V> Map<K, V> getMap(final String columnLabel, Class<K> keyClass, final Class<V> valueClass) {
        return getMap(this.rowMeta.getColumnIndex(columnLabel), keyClass, valueClass);
    }

    @Override
    public final Object getNonNull(final int indexBaseZero) throws JdbdSQLException, NullPointerException {
        final Object value;
        value = get(indexBaseZero);
        if (value == null) {
            throw new NullPointerException(String.format("value of column[index:%s] is null.", indexBaseZero));
        }
        return value;
    }

    @Override
    public final <T> T getNonNull(final int indexBaseZero, final Class<T> columnClass)
            throws JdbdSQLException, UnsupportedConvertingException, NullPointerException {
        final T value;
        value = get(indexBaseZero, columnClass);
        if (value == null) {
            throw new NullPointerException(String.format("value of column[index:%s] is null.", indexBaseZero));
        }
        return value;
    }

    @Override
    public final Object getNonNull(final String columnLabel) {
        return getNonNull(this.rowMeta.getColumnIndex(columnLabel));
    }

    @Override
    public final <T> T getNonNull(final String columnLabel, final Class<T> columnClass) {
        return getNonNull(this.rowMeta.getColumnIndex(columnLabel), columnClass);
    }


    private <T> T convertNonNullValue(final int indexBaseZero, final Object nonNull, final Class<T> targetClass)
            throws UnsupportedConvertingException {
        final PgColumnMeta meta = this.rowMeta.columnMetaArray[indexBaseZero];
        final T value;
        if (meta.textFormat) {
            final Object v;
            v = parseColumnFromText((String) nonNull, meta);
            if (targetClass.isInstance(v)) {
                value = targetClass.cast(v);
            } else {
                value = super.convertNonNullValue(indexBaseZero, v, targetClass);
            }
        } else {
            value = super.convertNonNullValue(indexBaseZero, nonNull, targetClass);
        }
        return value;
    }

    @Override
    protected final <T> T convertToOther(final int indexBaseZero, final Object nonNull, final Class<T> targetClass)
            throws UnsupportedConvertingException {
        final T value;
        try {
            final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].pgType;
            if (targetClass == PgLine.class) {
                value = targetClass.cast(PgGeometries.line(nonNull.toString()));
            } else if (targetClass == Line.class) {
                value = targetClass.cast(PgGeometries.lineSegment(nonNull.toString()));
            } else if (targetClass == PgBox.class) {
                value = targetClass.cast(PgGeometries.box(nonNull.toString()));
            } else if (targetClass == LineString.class && pgType == PgType.PATH) {
                value = targetClass.cast(PgGeometries.path(nonNull.toString()));
            } else if (targetClass == PgPolygon.class && pgType == PgType.POLYGON) {
                value = targetClass.cast(PgGeometries.polygon(nonNull.toString()));
            } else if (targetClass == Circle.class && pgType == PgType.CIRCLES) {
                value = targetClass.cast(PgGeometries.circle(nonNull.toString()));
            } else {
                value = super.convertToOther(indexBaseZero, nonNull, targetClass);
            }
            return value;
        } catch (IllegalArgumentException e) {
            throw createValueCannotConvertException(e, indexBaseZero, targetClass);
        }
    }

    @Override
    protected final Object convertNonNullToArray(final int indexBaseZero, Object nonNull, Class<?> targetClass)
            throws UnsupportedConvertingException {
        final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].pgType;
        final PgType elementType = pgType.elementType();
        final Pair<Class<?>, Integer> pair = JdbdBinds.getArrayDimensions(targetClass);

        final Object parsedArray;
        if (pgType == PgType.UNSPECIFIED && pair.getFirst() == String.class) {

        } else if (elementType != null && elementType.javaType() == pair.getFirst()) {

        } else {
            throw createNotSupportedException(indexBaseZero, targetClass);
        }

        return null;
    }

    @Override
    protected final <T> List<T> convertNonNullToList(final int indexBaseZero, final Object nonNull
            , final Class<T> elementClass) throws UnsupportedConvertingException {
        final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].pgType;
        final List<T> list;
        if (pgType == PgType.TSVECTOR && elementClass == String.class) {
            list = convertTsvectorToList(indexBaseZero, nonNull);
        } else {
            list = super.convertNonNullToList(indexBaseZero, nonNull, elementClass);
        }
        return list;
    }


    @Override
    protected final BigDecimal convertToBigDecimal(final int indexBaseZero, final Object nonNull) {
        final BigDecimal value;
        if (nonNull instanceof String) {
            switch (this.rowMeta.columnMetaArray[indexBaseZero].pgType) {
                case MONEY:
                case MONEY_ARRAY:
                    value = convertMoneyToBigDecimal(indexBaseZero, (String) nonNull);
                    break;
                default:
                    value = super.convertToBigDecimal(indexBaseZero, nonNull);
            }
        } else {
            value = super.convertToBigDecimal(indexBaseZero, nonNull);
        }
        return value;
    }


    @Override
    protected UnsupportedConvertingException createValueCannotConvertException(Throwable cause, int indexBasedZero
            , Class<?> targetClass) {
        return null;
    }

    @Override
    protected ZoneOffset obtainZoneOffsetClient() {
        return null;
    }

    @Override
    protected final Charset obtainColumnCharset(final int indexBasedZero) {
        return this.adjutant.clientCharset();
    }

    @Override
    protected final TemporalAccessor convertStringToTemporalAccessor(final int indexBaseZero, String sourceValue
            , Class<?> targetClass) throws DateTimeException, UnsupportedConvertingException {
        return null;
    }

    @Override
    protected final TemporalAmount convertStringToTemporalAmount(final int indexBaseZero, String nonNull, Class<?> targetClass)
            throws DateTimeException, UnsupportedConvertingException {
        return Interval.parse(nonNull);
    }

    @Override
    protected String formatTemporalAccessor(TemporalAccessor temporalAccessor) throws DateTimeException {
        return null;
    }

    private int checkIndex(int indexBaseZero) {
        if (indexBaseZero < 0 || indexBaseZero >= this.columnValues.length) {
            throw new JdbdSQLException(new SQLException(
                    String.format("indexBaseZero[%s] out of bounds[0 ,%s).", indexBaseZero, this.columnValues.length)));
        }
        return indexBaseZero;
    }

    /**
     * @throws IllegalArgumentException when textValue parse occur error.
     */
    private Object parseColumnFromText(final String textValue, final PgColumnMeta meta)
            throws IllegalArgumentException {
        final Object value;
        switch (meta.pgType) {
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
            case BOOLEAN: {

            }
            break;
            case TIMESTAMP: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                switch (textValue.toLowerCase()) {
                    case PgConstant.INFINITY:
                    case PgConstant.NEG_INFINITY:
                        value = textValue;
                        break;
                    default:
                        value = LocalDateTime.parse(textValue, PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER);
                }

            }
            break;
            case DATE: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                switch (textValue.toLowerCase()) {
                    case PgConstant.INFINITY:
                    case PgConstant.NEG_INFINITY:
                        value = textValue;
                        break;
                    default:
                        value = LocalDate.parse(textValue, PgTimes.PG_ISO_LOCAL_DATE_FORMATTER);
                }
            }
            break;
            case TIME: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                value = LocalTime.parse(textValue, JdbdTimes.ISO_LOCAL_TIME_FORMATTER);
            }
            break;
            case TIMESTAMPTZ: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                switch (textValue.toLowerCase()) {
                    case PgConstant.INFINITY:
                    case PgConstant.NEG_INFINITY:
                        value = textValue;
                        break;
                    default:
                        value = PgTimes.parseIsoOffsetDateTime(textValue);
                }
            }
            break;
            case TIMETZ: {
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                switch (textValue.toLowerCase()) {
                    case PgConstant.INFINITY:
                    case PgConstant.NEG_INFINITY:
                        value = textValue;
                        break;
                    default:
                        value = PgTimes.parseIsoOffsetTime(textValue);
                }
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
            case BYTEA: {
                throw new IllegalArgumentException(String.format("%s have parsed.", meta.pgType));
            }
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
            case SMALLINT_ARRAY:
            case INTEGER_ARRAY:
            case BIGINT_ARRAY:
            case TEXT_ARRAY:
            case DECIMAL_ARRAY:
            case REAL_ARRAY:
            case DOUBLE_ARRAY:
            case BOOLEAN_ARRAY:
            case DATE_ARRAY:
            case TIME_ARRAY:
            case TIMETZ_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
            case BYTEA_ARRAY:
            case VARCHAR_ARRAY:
            case OID_ARRAY:
            case MONEY_ARRAY:
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
                    LOG.trace("read array type {}", meta.pgType);
                }
                value = textValue;
            }
            break;
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
     * @see #get(int)
     */
    private <T> T convertNonNullValue(final int indexBasedZero, final Object nonNull) {
        return null;
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

    /**
     * @see #parseColumnFromText(String, PgColumnMeta)
     */
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
     * @see #convertNonNullToList(int, Object, Class)
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
        for (final int i = 0, lastEndpointEnd = 0; i < charArray.length; i++) {
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
     * @see #convertToBigDecimal(int, Object)
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
            final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].pgType;
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
        final PgType pgType = this.rowMeta.columnMetaArray[indexBaseZero].pgType;
        return new UnsupportedConvertingException(m, pgType, List.class);
    }

    private UnsupportedConvertingException moneyCannotConvertException(final int indexBasedZero) {
        PgType pgType = this.rowMeta.columnMetaArray[indexBasedZero].pgType;
        String format;
        format = "%s.getCurrencyInstance(Locale) method don't return %s instance,so can't convert postgre %s type to java type BigDecimal,jdbd-postgre need to upgrade.";
        String msg = String.format(format
                , NumberFormat.class.getName()
                , DecimalFormat.class.getName()
                , pgType);
        return new UnsupportedConvertingException(msg, pgType, BigDecimal.class);
    }

    static JdbdSQLException createResponseTextColumnValueError(PgColumnMeta meta, String textValue) {
        String m = String.format("Server response text value[%s] error for PgColumnMeta[%s].", textValue, meta);
        return new JdbdSQLException(new SQLException(m));
    }


}
