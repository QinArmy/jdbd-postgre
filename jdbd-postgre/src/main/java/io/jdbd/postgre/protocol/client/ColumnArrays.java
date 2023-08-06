package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgStrings;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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


    /**
     * @param nonNull     true : element of one dimension array non-null
     * @param elementFunc <ul>
     *                    <li>offset is non-whitespace,non-whitespace before end</li>
     *                    <li>no notation <strong>null</strong>,because have handled</li>
     *                    </ul>
     */
    public static Object parseArray(final String text, final boolean nonNull, final TextFunction<?> elementFunc,
                                    final char delimiter, final SqlType sqlType, final MappingType type,
                                    final ErrorHandler handler) throws IllegalArgumentException {

        if (!(type instanceof MappingType.SqlArrayType)) {
            throw _Exceptions.notArrayMappingType(type);
        }

        final Class<?> arrayJavaType;
        if (type instanceof UnaryGenericsMapping.ListMapping) {
            final Class<?> elementType;
            elementType = ((UnaryGenericsMapping.ListMapping<?>) type).genericsType();
            arrayJavaType = ArrayUtils.arrayClassOf(elementType);
        } else {
            arrayJavaType = type.javaType();
            if (!arrayJavaType.isArray()) {
                throw notArrayJavaType(type);
            }
        }

        final Object array, value;
        try {
            array = PostgreArrays.parseArrayText(arrayJavaType, text, nonNull, delimiter, elementFunc);
        } catch (Throwable e) {
            throw handler.apply(type, sqlType, text, e);
        }
        if (type instanceof UnaryGenericsMapping.ListMapping) {
            value = PostgreArrays.linearToList(array,
                    ((UnaryGenericsMapping.ListMapping<?>) type).listConstructor()
            );
        } else if (type.javaType().isInstance(array)) {
            value = array;
        } else {
            String m = String.format("%s return value and %s not match.", TextFunction.class.getName(), type);
            throw handler.apply(type, sqlType, text, new IllegalArgumentException(m));
        }
        return value;
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

    public static Map<Class<?>, Integer> parseArrayLengthMap(final Class<?> javaType, final String text, final int offset,
                                                             final int end) {
        final char colon = ':';
        final Map<Class<?>, Integer> map = _Collections.hashMap();

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
        } else if (map.size() != ArrayUtils.dimensionOf(javaType)) {
            throw boundDecorationNotMatch(javaType, text.substring(offset, end));
        }
        return _Collections.unmodifiableMap(map);
    }


    /**
     * <p>
     * parse postgre array text.
     * </p>
     *
     * @see <a href="https://www.postgresql.org/docs/15/arrays.html#ARRAYS-IO">Array Input and Output Syntax</a>
     */
    static Object parseArrayText(final Class<?> javaType, final String text, final boolean nonNull, final char delimiter,
                                 final TextFunction<?> function) throws IllegalArgumentException {
        final int length;
        length = text.length();
        int offset = 0;
        for (; offset < length; offset++) {
            if (!Character.isWhitespace(text.charAt(offset))) {
                break;
            }
        }
        if (offset == length) {
            throw new IllegalArgumentException("no text");
        }

        final Map<Class<?>, Integer> lengthMap;
        if (text.charAt(offset) == PgConstant.LEFT_SQUARE_BRACKET) {
            final int index;
            index = text.indexOf('=', offset);
            if (index < offset || index >= length) {
                throw new IllegalArgumentException("postgre array format error.");
            }
            lengthMap = parseArrayLengthMap(javaType, text, offset, index);
            offset = index + 1;
        } else {
            lengthMap = EMPTY_LENGTHS;
        }
        return _parseArray(javaType, text, nonNull, offset, length, delimiter, lengthMap, false, function);
    }


    @SuppressWarnings("unchecked")
    static <E> List<E> linearToList(final Object array, final Supplier<List<E>> supplier) {

        List<E> list;
        list = supplier.get();
        final int arrayLength;
        arrayLength = Array.getLength(array);
        for (int i = 0; i < arrayLength; i++) {
            list.add((E) Array.get(array, i));
        }
        if (list instanceof ImmutableSpec) {
            switch (arrayLength) {
                case 0:
                    list = _Collections.emptyList();
                    break;
                case 1:
                    list = _Collections.singletonList((E) Array.get(array, 0));
                    break;
                default:
                    list = _Collections.unmodifiableList(list);
            }
        }
        return list;
    }


    /**
     * <p>
     * parse postgre array text.
     * </p>
     *
     * @param nonNull true : if text multi range
     * @see <a href="https://www.postgresql.org/docs/15/arrays.html#ARRAYS-IO">Array Input and Output Syntax</a>
     */
    @Nullable
    private static Object _parseArray(final DataType dataType, final Class<?> javaType, final String text,
                                      final int offset, final int end, final char delimiter,
                                      final boolean doubleQuoteEscapes, Map<Class<?>, Integer> lengthMap,
                                      final boolean nonNull, final TextFunction<?> function) {

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
                    throw new IllegalArgumentException(m);
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
                        throw new IllegalArgumentException(m);
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


    private interface TextFunction<T> {

        T apply(String text, int offset, int end);

    }


}
