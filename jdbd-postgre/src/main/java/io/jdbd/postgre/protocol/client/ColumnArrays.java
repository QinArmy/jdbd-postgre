package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import org.qinarmy.util.FastStack;
import org.qinarmy.util.Stack;

import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

abstract class ColumnArrays {

    private ColumnArrays() {
        throw new UnsupportedOperationException();
    }

    private static final char LEFT_PAREN = '{';
    private static final char COMMA = ',';
    private static final char RIGHT_PAREN = '}';

    static Object readArray(final String value, final PgColumnMeta meta) {
        return null;
    }

    static Object readShortArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            if (charArray[index] != LEFT_PAREN) {
                throw new IllegalArgumentException("index error.");
            }
            final List<Short> list = new LinkedList<>();
            int i = index + 1, endIndex = -1;
            char ch;
            for (int from, to; i < charArray.length; i++) {
                if (Character.isWhitespace(charArray[i])) {
                    continue;
                }
                from = i;
                to = -1;
                for (int j = from + 1; j < charArray.length; j++) {
                    ch = charArray[j];
                    if (ch == COMMA) {
                        to = j;
                        break;
                    } else if (ch == RIGHT_PAREN) {
                        to = j;
                        break;
                    }
                }
                if (to < 0) {
                    throw arrayFormatError(meta);
                }
                if (to - from == 4 && isNull(charArray, from)) {
                    list.add(null);
                } else {
                    list.add(Short.parseShort(new String(charArray, from, to - from)));
                }
                if (charArray[to] == RIGHT_PAREN) {
                    endIndex = to;
                    break;
                }
            }
            if (endIndex < 0) {
                throw arrayFormatError(meta);
            }
            return new ArrayPair(list.toArray(new Short[0]), endIndex);
        };
        return readArray(value, meta, function);
    }


    private static Object readArray(final String value, final PgColumnMeta meta
            , final BiFunction<char[], Integer, ArrayPair> function) {
        final char[] charArray = value.toCharArray();
        //1. parse array dimension.
        final int dimension;
        dimension = getArrayDimension(charArray, meta);
        //2. parse array
        final Object array;
        if (dimension > 1) {
            array = readMultiDimensionArray(dimension, charArray, meta, function);
        } else {
            final ArrayPair pair;
            pair = function.apply(charArray, 0);
            for (int i = pair.index + 1; i < charArray.length; i++) {
                if (!Character.isWhitespace(charArray[i])) {
                    throw arrayFormatError(meta);
                }
            }
            array = pair.array;
        }
        return array;
    }

    private static Object readMultiDimensionArray(final int dimension, final char[] charArray
            , final PgColumnMeta meta, final BiFunction<char[], Integer, ArrayPair> function) {
        if (dimension < 2) {
            throw new IllegalArgumentException("dimension error");
        }
        final PgType elementType = Objects.requireNonNull(meta.pgType.elementType(), "elementType");

        final Stack<List<Object>> arrayStack = new FastStack<>();
        arrayStack.push(new LinkedList<>());
        char ch;
        for (int i = 0, dimensionIndex = 1; i < charArray.length; i++) {
            ch = charArray[i];
            if (ch != LEFT_PAREN) {
                if (!Character.isWhitespace(ch)) {
                    throw arrayFormatError(meta);
                }
                continue;
            }
            dimensionIndex++;
            if (dimensionIndex < dimension) {
                arrayStack.push(new LinkedList<>());
                if (dimensionIndex != arrayStack.size()) {
                    // here bug
                    String m = String.format("array parse error,dimensionIndex[%s],arrayStack.size[%s]."
                            , dimensionIndex, arrayStack.size());
                    throw new IllegalStateException(m);
                }
                continue;
            }
            // read one dimension array
            final ArrayPair pair;
            pair = function.apply(charArray, i);
            dimensionIndex--;
            i = pair.index;
            if (charArray[i] != RIGHT_PAREN) {
                throw new IllegalArgumentException(String.format("function[%s] error.", function));
            }
            final Class<?> arrayCass = pair.array.getClass();
            if (!arrayCass.isArray() || arrayCass.getComponentType() != elementType.javaType()) {
                throw new IllegalArgumentException(String.format("function[%s] error.", function));
            }
            arrayStack.peek().add(pair.array);

            for (int j = i + 1; j < charArray.length; j++) {
                ch = charArray[j];
                if (ch == COMMA) {
                    i = j;
                    break;
                } else if (ch == RIGHT_PAREN) {
                    i = j;
                    if (arrayStack.size() > 1) {
                        final List<Object> list = arrayStack.pop();
                        arrayStack.peek().add(list);
                        dimensionIndex--;
                    }
                    break;
                } else if (!Character.isWhitespace(ch)) {
                    throw arrayFormatError(meta);
                }
            }

            if (i == pair.index) {
                throw arrayFormatError(meta);
            }
        }
        if (arrayStack.size() != 1) {
            // here bug
            throw new IllegalStateException("parse array occur error.");
        }
        return createArray(dimension, elementType, arrayStack.pop());
    }


    private static int getArrayDimension(final char[] charArray, final PgColumnMeta meta) {
        int dimension = 0;
        for (char c : charArray) {
            if (c == '{') {
                dimension++;
            } else if (!Character.isWhitespace(c)) {
                break;
            }
        }
        if (dimension < 1) {
            throw arrayFormatError(meta);
        }
        return dimension;
    }


    private static Object createArray(final int dimension, final PgType elementType, final List<Object> valueList) {
        final String className = elementType.javaType().getName();

        final StringBuilder builder = new StringBuilder(dimension + 2 + className.length());
        for (int i = 0; i < dimension; i++) {
            builder.append('[');
        }
        builder.append('L')
                .append(className)
                .append(';');
        try {
            final Object array;
            array = Array.newInstance(Class.forName(builder.toString()), valueList.size());
            int index = 0;
            for (Object value : valueList) {
                Array.set(array, index, value);
                index++;
            }
            return array;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean isNull(final char[] charArray, int from) {
        return (charArray[from] == 'n' || charArray[from] == 'N')
                && (charArray[++from] == 'u' || charArray[from] == 'U')
                && (charArray[++from] == 'l' || charArray[from] == 'L')
                && (charArray[++from] == 'l' || charArray[from] == 'L');
    }


    private static PgJdbdException arrayFormatError(final PgColumnMeta meta) {
        throw new PgJdbdException(String.format("Postgre server response %s value error,couldn't parse,ColumnMeta[%s]"
                , meta.pgType, meta));
    }

    private static final class ArrayPair {
        private final Object array;

        private final int index;

        private ArrayPair(Object array, int index) {
            this.array = array;
            this.index = index;
        }

    }


}
