package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.util.*;
import io.jdbd.result.ResultRow;
import io.jdbd.type.Interval;
import io.jdbd.type.Point;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.*;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @since 1.0
 */
abstract class ColumnArrays {

    private ColumnArrays() {
        throw new UnsupportedOperationException();
    }


    private static final Map<Class<?>, Integer> EMPTY_LENGTHS = Collections.emptyMap();


    @SuppressWarnings("unchecked")
    static <T> T parseArray(final String source, final PgColumnMeta meta, final PgRowMeta rowMeta,
                            final Class<T> arrayClass) {
        final DataType dataType = meta.dataType;
        final Class<?> componentType;
        componentType = PgArrays.underlyingComponent(arrayClass);
        if (!(dataType instanceof PgType)) {
            if (componentType != String.class) { //TODO add hstore?
                throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
            }
            return (T) parseArrayText(dataType, arrayClass, source, false, PgConstant.COMMA, String::substring);
        }
        final TextFunction function;
        switch ((PgType) dataType) {
            case BOOLEAN_ARRAY: {
                if (componentType != boolean.class && componentType != Boolean.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = ColumnArrays::readBoolean;
            }
            break;
            case SMALLINT_ARRAY: {
                if (componentType != short.class && componentType != Short.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> Short.parseShort(text.substring(offset, end));
            }
            break;
            case INTEGER_ARRAY: {
                if (componentType != int.class && componentType != Integer.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> Integer.parseInt(text.substring(offset, end));
            }
            break;
            case OID_ARRAY:
            case BIGINT_ARRAY: {
                if (componentType != long.class && componentType != Long.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> Long.parseLong(text.substring(offset, end));
            }
            break;
            case DECIMAL_ARRAY: {
                if (componentType != BigDecimal.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> new BigDecimal(text.substring(offset, end));
            }
            break;
            case REAL_ARRAY: {
                if (componentType != float.class && componentType != Float.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> Float.parseFloat(text.substring(offset, end));
            }
            break;
            case DOUBLE_ARRAY: {
                if (componentType != double.class && componentType != Double.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> Double.parseDouble(text.substring(offset, end));
            }
            break;
            case BYTEA_ARRAY: {
                if (componentType != byte.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> {
                    final String element;
                    if (text.charAt(offset) == PgConstant.BACK_SLASH && text.charAt(offset + 1) == 'x') {
                        element = text.substring(offset + 2, end);
                    } else {
                        element = text.substring(offset, end);
                    }
                    final byte[] bytes;
                    bytes = element.getBytes(StandardCharsets.UTF_8);
                    return PgBuffers.decodeHex(bytes, bytes.length);
                };
            }
            break;
            case TIME_ARRAY: {
                if (componentType != LocalTime.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> {
                    // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
                    return LocalTime.parse(text.substring(offset, end), PgTimes.TIME_FORMATTER_6);
                };

            }
            break;
            case TIMETZ_ARRAY: {
                if (componentType != OffsetTime.class) {
                    throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                }
                function = (text, offset, end) -> {
                    // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
                    return OffsetTime.parse(text.substring(offset, end), PgTimes.OFFSET_TIME_FORMATTER_6);
                };
            }
            break;
            case DATE_ARRAY: {
                function = (text, offset, end) -> {
                    // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
                    final String element = text.substring(offset, end);
                    final Object value;
                    if (componentType == LocalDate.class) {
                        value = LocalDate.parse(element);
                    } else if (componentType != Object.class) {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    } else if (PgConstant.INFINITY.equalsIgnoreCase(element) || PgConstant.NEG_INFINITY.equalsIgnoreCase(element)) {
                        value = element;
                    } else {
                        value = LocalDate.parse(element);
                    }
                    return value;
                };
            }
            break;
            case TIMESTAMP_ARRAY: {
                function = (text, offset, end) -> {
                    // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
                    final String element = text.substring(offset, end);
                    final Object value;
                    if (componentType == LocalDateTime.class) {
                        value = LocalDateTime.parse(element, PgTimes.DATETIME_FORMATTER_6);
                    } else if (componentType != Object.class) {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    } else if (PgConstant.INFINITY.equalsIgnoreCase(element) || PgConstant.NEG_INFINITY.equalsIgnoreCase(element)) {
                        value = element;
                    } else {
                        value = LocalDateTime.parse(element, PgTimes.DATETIME_FORMATTER_6);
                    }
                    return value;
                };
            }
            break;
            case TIMESTAMPTZ_ARRAY: {
                function = (text, offset, end) -> {
                    // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
                    final String element = text.substring(offset, end);
                    final Object value;
                    if (componentType == OffsetDateTime.class) {
                        value = OffsetDateTime.parse(element, PgTimes.OFFSET_DATETIME_FORMATTER_6);
                    } else if (componentType != Object.class) {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    } else if (PgConstant.INFINITY.equalsIgnoreCase(element) || PgConstant.NEG_INFINITY.equalsIgnoreCase(element)) {
                        value = element;
                    } else {
                        value = OffsetDateTime.parse(element, PgTimes.OFFSET_DATETIME_FORMATTER_6);
                    }
                    return value;
                };
            }
            break;
            case INTERVAL_ARRAY: {
                function = (text, offset, end) -> {
                    // currently jdbd-postgre support only iso style,see io.jdbd.postgre.protocol.client.PgConnectionTask
                    final Object value;
                    if (componentType == Interval.class) {
                        value = Interval.parse(text.substring(offset, end), true);
                    } else if (componentType == String.class) {
                        value = text.substring(offset, end);
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    }
                    return value;
                };
            }
            break;
            case MONEY_ARRAY: {
                function = (text, offset, end) -> {
                    final Object value;
                    if (componentType == String.class) {
                        value = text.substring(offset, end);
                    } else if (componentType == BigDecimal.class) {
                        value = readMoney(text.substring(offset, end), meta, rowMeta.moneyFormat);
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    }
                    return value;
                };
            }
            break;
            case UUID_ARRAY: {
                function = (text, offset, end) -> {
                    final Object value;
                    if (componentType == String.class) {
                        value = text.substring(offset, end);
                    } else if (componentType == UUID.class) {
                        value = UUID.fromString(text.substring(offset, end));
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    }
                    return value;
                };
            }
            break;
            case BIT_ARRAY:
            case VARBIT_ARRAY: {
                function = (text, offset, end) -> {
                    final Object value;
                    if (componentType == String.class) {
                        value = text.substring(offset, end);
                    } else if (componentType == BitSet.class) {
                        value = PgStrings.bitStringToBitSet(text.substring(offset, end), true);
                    } else if (componentType == Long.class) {
                        value = Long.parseLong(text.substring(offset, end), 2);
                    } else if (componentType == Integer.class) {
                        value = Integer.parseInt(text.substring(offset, end), 2);
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    }
                    return value;
                };
            }
            break;
            case POINT_ARRAY: {
                function = (text, offset, end) -> {
                    final Object value;
                    if (componentType == String.class) {
                        value = text.substring(offset, end);
                    } else if (componentType == Point.class) {
                        value = PgGeometries.point(text.substring(offset, end));
                    } else {
                        throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);
                    }
                    return value;
                };
            }
            break;
            case CHAR_ARRAY:
            case VARCHAR_ARRAY:
            case TEXT_ARRAY:
            case JSON_ARRAY:
            case JSONB_ARRAY:
            case JSONPATH_ARRAY:
            case XML_ARRAY:
            case TSQUERY_ARRAY:
            case TSVECTOR_ARRAY:

            case INT4RANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case NUMRANGE_ARRAY:
            case DATERANGE_ARRAY:
            case TSRANGE_ARRAY:
            case TSTZRANGE_ARRAY:

            case INT4MULTIRANGE_ARRAY:
            case INT8MULTIRANGE_ARRAY:
            case NUMMULTIRANGE_ARRAY:
            case DATEMULTIRANGE_ARRAY:
            case TSMULTIRANGE_ARRAY:
            case TSTZMULTIRANGE_ARRAY:

            case LINE_ARRAY:
            case PATH_ARRAY:
            case BOX_ARRAY:
            case LSEG_ARRAY:
            case CIRCLE_ARRAY:
            case POLYGON_ARRAY:

            case CIDR_ARRAY:
            case INET_ARRAY:
            case MACADDR_ARRAY:
            case MACADDR8_ARRAY:

            case REF_CURSOR_ARRAY: //TODO REF_CURSOR_ARRAY add api ?
                function = String::substring;
                break;
            default:
                throw PgExceptions.cannotConvertColumnValue(meta, source, arrayClass, null);

        }


        return (T) parseArrayText(dataType, arrayClass, source, componentType.isPrimitive(), ',', function);
    }

    private static boolean readBoolean(final String text, int offset, int end) {
        final boolean value;
        final String element = text.substring(offset, end);
        if (PgConstant.TRUE.equalsIgnoreCase(element)) {
            value = true;
        } else if (PgConstant.FALSE.equalsIgnoreCase(element)) {
            value = false;
        } else {
            throw new IllegalArgumentException("server response boolean array");
        }
        return value;
    }


    private static BigDecimal readMoney(final String element, final PgColumnMeta meta, final DecimalFormat format) {
        try {
            final Number value;
            value = format.parse(element);
            if (!(value instanceof BigDecimal)) {
                throw moneyCannotConvertException(meta);
            }
            return (BigDecimal) value;
        } catch (Throwable e) {
            final String columnLabel = meta.columnLabel;
            String m;
            m = String.format("Column[%s] postgre %s type convert to  java type BigDecimal failure.", columnLabel,
                    meta.dataType);
            if (e instanceof ParseException) {
                m = String.format("%s\nYou possibly execute '%s' AND %s.get(\"%s\",%s.class) in multi-statement.",
                        m,
                        "SET lc_monetary",
                        ResultRow.class.getName(),
                        columnLabel,
                        BigDecimal.class.getName()
                );
            }
            throw new JdbdException(m);
        }
    }


    /**
     * <p>
     * parse postgre array text.
     * </p>
     *
     * @see <a href="https://www.postgresql.org/docs/15/arrays.html#ARRAYS-IO">Array Input and Output Syntax</a>
     */
    private static Object parseArrayText(final DataType dataType, final Class<?> javaType, final String text,
                                         final boolean nonNull, final char delimiter, final TextFunction function)
            throws JdbdException {
        final int length;
        length = text.length();
        int offset = 0;
        for (; offset < length; offset++) {
            if (!Character.isWhitespace(text.charAt(offset))) {
                break;
            }
        }
        if (offset == length) {
            throw new JdbdException("array no text");
        }

        final Map<Class<?>, Integer> lengthMap;
        if (text.charAt(offset) == PgConstant.LEFT_SQUARE_BRACKET) {
            final int index;
            index = text.indexOf('=', offset);
            if (index < offset || index >= length) {
                throw new JdbdException("postgre array format error.");
            }
            lengthMap = parseArrayLengthMap(javaType, text, offset, index);
            offset = index + 1;
        } else {
            lengthMap = EMPTY_LENGTHS;
        }
        return _parseArray(dataType, javaType, text, offset, length, delimiter, false, lengthMap, nonNull, function);
    }


    private static Map<Class<?>, Integer> parseArrayLengthMap(final Class<?> javaType, final String text, final int offset,
                                                              final int end) {
        final char colon = ':';
        final Map<Class<?>, Integer> map = PgCollections.hashMap();

        Class<?> componentType;
        componentType = javaType;

        boolean inBracket = false, afterColon = false;
        char ch;
        for (int i = offset, lowerIndex = -1, upperIndex = -1, lower = 0, length; i < end; i++) {
            ch = text.charAt(i);
            if (!inBracket) {
                if (ch == PgConstant.LEFT_SQUARE_BRACKET) {
                    inBracket = true;
                } else if (!Character.isWhitespace(ch)) {
                    throw isNotWhitespaceError(i);
                }
            } else if (lowerIndex < 0) {
                if (!Character.isWhitespace(ch)) {
                    lowerIndex = i;
                }
            } else if (lowerIndex > 0) {
                if (ch == colon || Character.isWhitespace(ch)) {
                    lower = Integer.parseInt(text.substring(lowerIndex, i));
                    lowerIndex = 0;
                }
                if (ch == colon) {
                    afterColon = true;
                }
            } else if (!afterColon) {
                if (ch == colon) {
                    afterColon = true;
                } else if (!Character.isWhitespace(ch)) {
                    throw lengthOfDimensionError(text.substring(offset, end));
                }
            } else if (upperIndex < 0) {
                if (!Character.isWhitespace(ch)) {
                    upperIndex = i;
                }
            } else if (upperIndex == 0) {
                if (ch == PgConstant.RIGHT_SQUARE_BRACKET) {
                    inBracket = false;
                    lowerIndex = upperIndex = -1;
                } else if (!Character.isWhitespace(ch)) {
                    throw lengthOfDimensionError(text.substring(offset, end));
                }
            } else if (ch == PgConstant.RIGHT_SQUARE_BRACKET || Character.isWhitespace(ch)) {
                length = Integer.parseInt(text.substring(upperIndex, i)) - lower + 1;
                if (length < 0) {
                    throw lengthOfDimensionError(text.substring(offset, end));
                }
                upperIndex = 0;
                if (map.putIfAbsent(componentType, length) != null) {
                    throw boundDecorationNotMatch(javaType, text.substring(offset, end));
                }
                if (componentType.isArray()) {
                    componentType = componentType.getComponentType();
                }
                if (ch == PgConstant.RIGHT_SQUARE_BRACKET) {
                    inBracket = false;
                    lowerIndex = upperIndex = -1;
                }
            }


        }//for

        if (inBracket) {
            throw lengthOfDimensionError(text.substring(offset, end));
        } else if (map.size() != PgArrays.dimensionOf(javaType)) {
            throw boundDecorationNotMatch(javaType, text.substring(offset, end));
        }
        return PgCollections.unmodifiableMap(map);
    }


    private static int parseArrayLength(final String text, final int offset, final int end) throws JdbdException {
        char ch;
        boolean leftBrace = true, inBrace = false, inQuote = false, arrayEnd = false;

        int length = 0;
        for (int i = offset, itemCount = 0; i < end; i++) {
            ch = text.charAt(i);
            if (leftBrace) {
                if (ch == PgConstant.LEFT_BRACE) {
                    leftBrace = false;
                } else if (!Character.isWhitespace(ch)) {
                    throw isNotWhitespaceError(i);
                }
            } else if (inQuote) {
                if (ch == PgConstant.BACK_SLASH) {
                    i++;
                } else if (ch == PgConstant.DOUBLE_QUOTE) {
                    inQuote = false;
                }
            } else if (inBrace) {
                if (ch == PgConstant.RIGHT_BRACE) {
                    inBrace = false;
                }
            } else if (ch == PgConstant.LEFT_BRACE) {
                itemCount++;
                inBrace = true;
            } else if (ch == PgConstant.DOUBLE_QUOTE) {
                itemCount++;
                inQuote = true;
            } else if (ch == PgConstant.COMMA) {
                length++;
            } else if (ch == PgConstant.RIGHT_BRACE) {
                if (itemCount > 0) {
                    length++;
                }
                arrayEnd = true;
                break;
            } else if (itemCount == 0 && !Character.isWhitespace(ch)) {
                itemCount++;
            }

        }
        if (leftBrace) {
            throw noLeftBrace(offset);
        } else if (!arrayEnd) {
            throw noRightBrace(end);
        }
        return length;
    }


    /**
     * <p>
     * parse postgre array text.
     * </p>
     *
     * @param nonNull true : if text multi range
     * @see <a href="https://www.postgresql.org/docs/15/arrays.html#ARRAYS-IO">Array Input and Output Syntax</a>
     */
    private static Object _parseArray(final DataType dataType, final Class<?> javaType, final String text,
                                      final int offset, final int end, final char delimiter,
                                      final boolean doubleQuoteEscapes, Map<Class<?>, Integer> lengthMap,
                                      final boolean nonNull, final TextFunction function) {

        assert offset >= 0 && offset < end;

        final Class<?> componentType;
        componentType = javaType.getComponentType();

        final boolean oneDimension;
        if (dataType == PgType.BYTEA_ARRAY) {
            oneDimension = componentType.isArray() && !componentType.getComponentType().isArray();
        } else {
            oneDimension = !componentType.isArray();
        }
        final int arrayLength;
        if (lengthMap == EMPTY_LENGTHS) {
            arrayLength = parseArrayLength(text, offset, end);
        } else {
            arrayLength = lengthMap.get(javaType);
        }
        final Object array;
        array = Array.newInstance(componentType, arrayLength);
        if (arrayLength == 0) {
            return array;
        }

        Object elementValue;
        boolean leftBrace = true, inBrace = false, inQuote = false, inElementBrace = false, arrayEnd = false;
        char ch, startChar = PgConstant.NUL, preChar = PgConstant.NUL;
        for (int i = offset, startIndex = -1, arrayIndex = 0, tailIndex, nextIndex; i < end; i++) {
            ch = text.charAt(i);
            if (leftBrace) {
                if (ch == PgConstant.LEFT_BRACE) {
                    leftBrace = false;
                } else if (!Character.isWhitespace(ch)) {
                    throw isNotWhitespaceError(i);
                }
            } else if (inQuote) {
                if (ch == PgConstant.BACK_SLASH) {
                    i++;
                } else if (ch == PgConstant.DOUBLE_QUOTE) {
                    if (doubleQuoteEscapes
                            && (nextIndex = i + 1) < end
                            && text.charAt(nextIndex) == PgConstant.DOUBLE_QUOTE) {
                        i++;
                    } else {
                        inQuote = false;
                        if (oneDimension) {
                            assert startIndex > 0;
                            Array.set(array, arrayIndex++, function.apply(text, startIndex, i));
                            startIndex = -1;
                        }
                    }

                }
            } else if (inBrace) {
                if (ch == PgConstant.LEFT_BRACE) {
                    inElementBrace = true;
                } else if (ch == PgConstant.RIGHT_BRACE) {
                    if (inElementBrace) {
                        inElementBrace = false;
                    } else {
                        inBrace = false;
                        elementValue = _parseArray(dataType, componentType, text, startIndex, i + 1, delimiter,
                                doubleQuoteEscapes, lengthMap, nonNull, function);
                        Array.set(array, arrayIndex++, elementValue);
                        startIndex = -1;
                    }
                }
            } else if (ch == PgConstant.LEFT_BRACE) {
                if (oneDimension) {
                    String m = String.format("postgre array isn't one dimension array after offset[%s]", i);
                    throw new JdbdException(m);
                }
                assert startIndex < 0;
                startIndex = i;
                inBrace = true;
                inElementBrace = false;
            } else if (ch == PgConstant.DOUBLE_QUOTE) {
                inQuote = true;
                if (oneDimension) {
                    assert startIndex < 0;
                    startIndex = i + 1;
                }
            } else if (ch == delimiter || ch == PgConstant.RIGHT_BRACE) {
                if (startIndex > 0) {
                    assert oneDimension;
                    if ((startChar == 'n' || startChar == 'N')
                            && (tailIndex = startIndex + 4) <= i
                            && text.regionMatches(true, startIndex, PgConstant.NULL, 0, 4)
                            && (tailIndex == i || PgStrings.isWhitespace(text, tailIndex, i))) {
                        if (nonNull) {
                            String m = String.format("%s element must be non-null,but server return null", dataType);
                            throw new JdbdException(m);
                        }
                        Array.set(array, arrayIndex++, null);
                    } else if (Character.isWhitespace(preChar)) {
                        for (tailIndex = i - 2; tailIndex > startIndex; tailIndex--) {
                            if (!Character.isWhitespace(text.charAt(tailIndex))) {
                                break;
                            }
                        }
                        Array.set(array, arrayIndex++, function.apply(text, startIndex, tailIndex + 1));
                    } else {
                        Array.set(array, arrayIndex++, function.apply(text, startIndex, i));
                    }
                    startIndex = -1;
                }
                if (ch == PgConstant.RIGHT_BRACE) {
                    arrayEnd = true;
                    break;
                }
            } else if (startIndex < 0) {
                if (!Character.isWhitespace(ch)) {
                    if (!oneDimension) {
                        String m = String.format("postgre array isn't multi-dimension array after offset[%s]", i);
                        throw new JdbdException(m);
                    }
                    startChar = ch;
                    startIndex = i;
                }
            }

            preChar = ch;

        }//for

        if (leftBrace) {
            throw noLeftBrace(offset);
        } else if (!arrayEnd) {
            throw noRightBrace(end);
        }
        return array;
    }


    private static JdbdException boundDecorationNotMatch(Class<?> javaType, String decoration) {
        String m = String.format("postgre bound decoration %s and %s not match.", decoration, javaType.getName());
        return new JdbdException(m);
    }


    private static JdbdException lengthOfDimensionError(String lengths) {
        String m = String.format("postgre bound decoration %s error.", lengths);
        return new JdbdException(m);
    }


    private static JdbdException noRightBrace(int end) {
        return new JdbdException(String.format("postgre array no right brace before offset[%s] nearby", end));
    }

    private static JdbdException noLeftBrace(int offset) {
        return new JdbdException(String.format("postgre array no left brace at offset[%s] nearby", offset));
    }

    private static JdbdException isNotWhitespaceError(int offset) {
        return new JdbdException(String.format("postgre array error at offset[%s]", offset));
    }

    private static JdbdException moneyCannotConvertException(final PgColumnMeta meta) {
        final String format;
        format = "Column[index:%s,label:%s] %s.getCurrencyInstance(Locale) method don't return %s instance,so can't convert postgre %s type to java type BigDecimal,jdbd-postgre need to upgrade.";
        String msg = String.format(format,
                meta.columnIndex,
                meta.columnLabel,
                NumberFormat.class.getName(),
                DecimalFormat.class.getName(),
                meta.dataType);
        return new JdbdException(msg);
    }


    interface TextFunction {

        Object apply(String text, int offset, int end);

    }


}
