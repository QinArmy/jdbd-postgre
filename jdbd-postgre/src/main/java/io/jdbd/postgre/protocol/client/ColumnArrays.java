package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgBox;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.type.PgLine;
import io.jdbd.postgre.type.PgPolygon;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.postgre.util.PgBuffers;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.UnsupportedConvertingException;
import io.jdbd.type.Interval;
import io.jdbd.type.Point;
import io.jdbd.type.geo.Line;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.LongString;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.qinarmy.util.FastStack;
import io.qinarmy.util.Pair;
import io.qinarmy.util.Stack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.time.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 */
abstract class ColumnArrays {

    private ColumnArrays() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(ColumnArrays.class);

    private static final char LEFT_PAREN = '{';
    private static final char COMMA = ',';
    private static final char RIGHT_PAREN = '}';
    private static final char DOUBLE_QUOTE = '"';

    private static final char BACKSLASH = '\\';

    static Object readBooleanArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Boolean.class && targetArrayClass != boolean.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Boolean> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    if (targetArrayClass == boolean.class) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(null);
                } else {
                    list.add(PgStrings.parseBoolean(nullable));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == Boolean.class) {
                array = list.toArray(new Boolean[0]);
            } else {
                final boolean[] booleans = new boolean[list.size()];
                int i = 0;
                for (Boolean b : list) {
                    booleans[i] = b;
                    i++;
                }
                array = booleans;
            }
            list.clear();
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }


    static Object readShortArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Short.class && targetArrayClass != short.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Short> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    if (targetArrayClass == short.class) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(null);
                } else {
                    list.add(Short.parseShort(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == Short.class) {
                array = list.toArray(new Short[0]);
            } else {
                final short[] shorts = new short[list.size()];
                int i = 0;
                for (Short v : list) {
                    shorts[i] = v;
                    i++;
                }
                array = shorts;
            }
            list.clear();
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readIntegerArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Integer.class && targetArrayClass != int.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Integer> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    if (targetArrayClass == int.class) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(null);
                } else {
                    list.add(Integer.parseInt(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == Integer.class) {
                array = list.toArray(new Integer[0]);
            } else {
                final int[] valueArray = new int[list.size()];
                int i = 0;
                for (Integer v : list) {
                    valueArray[i] = v;
                    i++;
                }
                array = valueArray;
            }
            list.clear();
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readBigIntArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Long.class && targetArrayClass != long.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Long> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    if (targetArrayClass == long.class) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(null);
                } else {
                    list.add(Long.parseLong(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == Long.class) {
                array = list.toArray(new Long[0]);
            } else {
                final long[] valueArray = new long[list.size()];
                int i = 0;
                for (Long v : list) {
                    valueArray[i] = v;
                    i++;
                }
                array = valueArray;
            }
            list.clear();
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readDecimalArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != BigDecimal.class && targetArrayClass != Object.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                    return;
                }
                if (nullable.equalsIgnoreCase(PgConstant.NaN)) {
                    if (targetArrayClass == BigDecimal.class) {
                        String m = String.format("%s couldn't convert to BigDecimal", nullable);
                        throw new IllegalArgumentException(m);
                    }
                    list.add(nullable);
                } else {
                    list.add(new BigDecimal(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == BigDecimal.class) {
                array = new BigDecimal[list.size()];
            } else {
                array = new Object[list.size()];
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readRealArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Float.class && targetArrayClass != float.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Float> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    if (targetArrayClass == float.class) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(null);
                } else {
                    list.add(Float.parseFloat(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == Float.class) {
                array = list.toArray(new Float[0]);
            } else {
                final float[] valueArray = new float[list.size()];
                int i = 0;
                for (Float v : list) {
                    valueArray[i] = v;
                    i++;
                }
                array = valueArray;
            }
            list.clear();
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readDoubleArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Double.class && targetArrayClass != double.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Double> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    if (targetArrayClass == double.class) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(null);
                } else {
                    list.add(Double.parseDouble(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == Double.class) {
                array = list.toArray(new Double[0]);
            } else {
                final double[] valueArray = new double[list.size()];
                int i = 0;
                for (Double v : list) {
                    valueArray[i] = v;
                    i++;
                }
                array = valueArray;
            }
            list.clear();
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readTimeArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<LocalTime> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    // @see PgConnectionTask
                    // startStartup Message set to default ISO
                    list.add(LocalTime.parse(nullable, PgTimes.ISO_LOCAL_TIME_FORMATTER));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new LocalTime[0]), endIndex);
        };
        return readArray(value, meta, function, LocalTime.class);
    }

    static Object readDateArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                    return;
                }
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                final Object parsedValue;
                parsedValue = PgTimes.parseIsoLocalDate(nullable);
                if (targetArrayClass == LocalDate.class) {
                    if (!(parsedValue instanceof LocalDate)) {
                        throw dateInfinityException(parsedValue, LocalDate.class, meta);
                    }
                    list.add(parsedValue);
                } else if (targetArrayClass == Object.class) {
                    list.add(parsedValue);
                } else if (targetArrayClass == String.class) {
                    list.add(parsedValue.toString());
                } else {
                    throw targetArrayClassError(targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == LocalDate.class) {
                array = new LocalDate[list.size()];
            } else if (targetArrayClass == Object.class) {
                array = new Object[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw targetArrayClassError(targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readTimestampArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                    return;
                }
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                final Object parsedValue;
                parsedValue = PgTimes.parseIsoLocalDateTime(nullable);
                if (targetArrayClass == LocalDateTime.class) {
                    if (!(parsedValue instanceof LocalDateTime)) {
                        throw dateInfinityException(parsedValue, LocalDateTime.class, meta);
                    }
                    list.add(parsedValue);
                } else if (targetArrayClass == Object.class) {
                    list.add(parsedValue);
                } else if (targetArrayClass == String.class) {
                    if (parsedValue instanceof LocalDateTime) {
                        list.add(((LocalDateTime) parsedValue).format(PgTimes.ISO_LOCAL_DATETIME_FORMATTER));
                    } else {
                        list.add(parsedValue);
                    }
                } else {
                    throw targetArrayClassError(targetArrayClass);
                }

            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == LocalDateTime.class) {
                array = new LocalDateTime[list.size()];
            } else if (targetArrayClass == Object.class) {
                array = new Object[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw targetArrayClassError(targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readTimeTzArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<OffsetTime> list = new LinkedList<>();

            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    // @see PgConnectionTask
                    // startStartup Message set to default ISO
                    list.add(PgTimes.parseIsoOffsetTime(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new OffsetTime[0]), endIndex);
        };
        return readArray(value, meta, function, OffsetTime.class);
    }

    static Object readTimestampTzArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                    return;
                }
                // @see PgConnectionTask
                // startStartup Message set to default ISO
                final Object parsedValue;
                parsedValue = PgTimes.parseIsoOffsetDateTime(nullable);
                if (targetArrayClass == OffsetDateTime.class) {
                    if (!(parsedValue instanceof OffsetDateTime)) {
                        throw dateInfinityException(parsedValue, OffsetDateTime.class, meta);
                    }
                    list.add(parsedValue);
                } else if (targetArrayClass == ZonedDateTime.class) {
                    if (!(parsedValue instanceof OffsetDateTime)) {
                        throw dateInfinityException(parsedValue, OffsetDateTime.class, meta);
                    }
                    list.add(((OffsetDateTime) parsedValue).toZonedDateTime());
                } else if (targetArrayClass == Object.class) {
                    list.add(parsedValue);
                } else if (targetArrayClass == String.class) {
                    if (parsedValue instanceof OffsetDateTime) {
                        list.add(((OffsetDateTime) parsedValue).format(PgTimes.ISO_OFFSET_DATETIME_FORMATTER));
                    } else {
                        list.add(parsedValue);
                    }
                } else {
                    throw targetArrayClassError(targetArrayClass);
                }

            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == OffsetDateTime.class) {
                array = new OffsetDateTime[list.size()];
            } else if (targetArrayClass == ZonedDateTime.class) {
                array = new ZonedDateTime[list.size()];
            } else if (targetArrayClass == Object.class) {
                array = new Object[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw targetArrayClassError(targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    /**
     * @throws io.jdbd.result.UnsupportedConvertingException when targetArrayClass and value not match.
     */
    static Object readBitArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    if (targetArrayClass == long.class || targetArrayClass == int.class) {
                        throw targetArrayClassError(targetArrayClass);
                    }
                    list.add(null);
                    return;
                }
                if (targetArrayClass == BitSet.class) {
                    list.add(PgStrings.bitStringToBitSet(nullable, false));
                } else if (targetArrayClass == Long.class || targetArrayClass == long.class) {
                    list.add(Long.parseUnsignedLong(PgStrings.reverse(nullable), 2));
                } else if (targetArrayClass == Integer.class || targetArrayClass == int.class) {
                    list.add(Integer.parseUnsignedInt(PgStrings.reverse(nullable), 2));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    throw targetArrayClassError(targetArrayClass);
                }

            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == BitSet.class) {
                final Object[] arrayValue = new BitSet[list.size()];
                array = list.toArray(arrayValue);
            } else if (targetArrayClass == Long.class) {
                final Object[] arrayValue = new Long[list.size()];
                array = list.toArray(arrayValue);
            } else if (targetArrayClass == Integer.class) {
                final Object[] arrayValue = new Integer[list.size()];
                array = list.toArray(arrayValue);
            } else if (targetArrayClass == int.class) {
                final int[] intArray = new int[list.size()];
                int i = 0;
                for (Object v : list) {
                    intArray[i] = (Integer) v;
                    i++;
                }
                array = intArray;
            } else if (targetArrayClass == long.class) {
                final long[] longArray = new long[list.size()];
                int i = 0;
                for (Object v : list) {
                    longArray[i] = (Long) v;
                    i++;
                }
                array = longArray;
            } else if (targetArrayClass == String.class) {
                final Object[] arrayValue = new String[list.size()];
                array = list.toArray(arrayValue);
            } else {
                throw targetArrayClassError(targetArrayClass);
            }
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    /**
     * @throws io.jdbd.result.UnsupportedConvertingException when targetArrayClass and value not match.
     */
    static Object readMoneyArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass
            , final @Nullable DecimalFormat format) {
        if (targetArrayClass != BigDecimal.class && targetArrayClass != String.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    list.add(PgResultRow.parseMoney(meta, nullable, format));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == BigDecimal.class) {
                array = new BigDecimal[list.size()];
            } else {
                array = new String[list.size()];
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }


    /**
     * @throws io.jdbd.result.UnsupportedConvertingException when targetArrayClass and value not match.
     */
    static Object readUuidArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != UUID.class && targetArrayClass != String.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == UUID.class) {
                    list.add(UUID.fromString(nullable));
                } else {
                    list.add(nullable);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == UUID.class) {
                array = new UUID[list.size()];
            } else {
                array = new String[list.size()];
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    /**
     * @throws io.jdbd.result.UnsupportedConvertingException when targetArrayClass and value not match.
     */
    static Object readByteaArray(final String value, final PgColumnMeta meta, final Charset charset
            , final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                    return;
                }
                byte[] bytes;
                if (nullable.startsWith("\\x")) {
                    bytes = nullable.substring(2).getBytes(charset);
                    bytes = PgBuffers.decodeHex(bytes, bytes.length);
                } else {
                    bytes = nullable.getBytes(charset);
                }
                if (targetArrayClass == byte[].class) {
                    list.add(bytes);
                } else if (targetArrayClass == LongBinary.class) {
                    list.add(LongBinaries.fromArray(bytes));
                } else if (targetArrayClass == String.class) {
                    list.add(new String(bytes, charset));
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == byte[].class) {
                array = new byte[list.size()][];
            } else if (targetArrayClass == LongBinary.class) {
                array = new LongBinary[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readTextArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {

        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else if (targetArrayClass == LongString.class) {
                    list.add(LongStrings.fromString(nullable));
                } else if (targetArrayClass.isEnum()) {
                    try {
                        list.add(PgStrings.parseEnumValue(targetArrayClass, nullable));
                    } catch (IllegalArgumentException e) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object array;
            if (targetArrayClass == String.class) {
                final Object[] v = new String[list.size()];
                list.toArray(v);
                array = v;
            } else if (targetArrayClass == LongString.class) {
                final Object[] v = new LongString[list.size()];
                list.toArray(v);
                array = v;
            } else if (targetArrayClass.isEnum()) {
                final int length = list.size();
                array = Array.newInstance(targetArrayClass, length);
                int i = 0;
                for (Object v : list) {
                    Array.set(array, i, v);
                    i++;
                }
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(array, endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readIntervalArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                    return;
                }
                final Interval v = Interval.parse(nullable, true);
                if (targetArrayClass == Interval.class) {
                    list.add(v);
                } else if (targetArrayClass == Duration.class) {
                    if (!v.isDuration()) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(v.toDurationExact());
                } else if (targetArrayClass == Period.class) {
                    if (!v.isPeriod()) {
                        throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                    }
                    list.add(v.toPeriodExact());
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == Interval.class) {
                array = new Interval[list.size()];
            } else if (targetArrayClass == Duration.class) {
                array = new Duration[list.size()];
            } else if (targetArrayClass == Period.class) {
                array = new Period[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }


    static Object readPointArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == Point.class) {
                    list.add(PgGeometries.point(nullable));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == Point.class) {
                array = new Point[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readLineArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == PgLine.class) {
                    list.add(PgGeometries.line(nullable));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == PgLine.class) {
                array = new PgLine[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readLineSegmentArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == Line.class) {
                    list.add(PgGeometries.lineSegment(nullable));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == Line.class) {
                array = new Line[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readBoxArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == PgBox.class) {
                    list.add(PgGeometries.box(nullable));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == PgBox.class) {
                array = new PgBox[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readPathArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == LineString.class) {
                    list.add(PgGeometries.path(nullable));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else if (targetArrayClass == LongString.class) {
                    list.add(LongStrings.fromString(nullable));
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == LineString.class) {
                array = new LineString[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else if (targetArrayClass == LongString.class) {
                array = new LongString[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readPolygonArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {

        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == PgPolygon.class) {
                    list.add(PgGeometries.polygon(nullable));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else if (targetArrayClass == LongString.class) {
                    list.add(LongStrings.fromString(nullable));
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == PgPolygon.class) {
                array = new PgPolygon[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else if (targetArrayClass == LongString.class) {
                array = new LongString[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readCirclesArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else if (targetArrayClass == Circle.class) {
                    list.add(PgGeometries.circle(nullable));
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    throw PgResultRow.notSupportConverting(meta, targetArrayClass);
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == Circle.class) {
                array = new Circle[list.size()];
            } else if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                throw PgResultRow.notSupportConverting(meta, targetArrayClass);
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    private static Object readArray(final String value, final PgColumnMeta meta
            , final BiFunction<char[], Integer, ArrayPair> function, final Class<?> targetArrayClass) {
        final char[] charArray = value.toCharArray();
        //1. parse array dimension.
        final Pair<Integer, Integer> dimensionPair = getArrayDimension(charArray, meta, targetArrayClass);
        final int dimension = dimensionPair.getFirst();
        int index = dimensionPair.getSecond();
        for (; index < charArray.length; index++) {
            if (!Character.isWhitespace(charArray[index])) {
                break;
            }
        }
        if (index == charArray.length || charArray[index] != '{') {
            throw arrayFormatError(meta);
        }
        //2. parse array
        final Object array;
        if (dimension > 1) {
            array = readMultiDimensionArray(dimension, charArray, index, meta, function, targetArrayClass);
        } else {
            final ArrayPair pair;
            pair = function.apply(charArray, index);
            for (int i = pair.index + 1; i < charArray.length; i++) {
                if (!Character.isWhitespace(charArray[i])) {
                    throw arrayFormatError(meta);
                }
            }
            array = pair.array;
        }
        return array;
    }

    /**
     * @return multi dimension array
     * @see #readArray(String, PgColumnMeta, BiFunction, Class)
     */
    private static Object readMultiDimensionArray(final int dimension, final char[] charArray
            , final int index, final PgColumnMeta meta
            , final BiFunction<char[], Integer, ArrayPair> function
            , final Class<?> targetArrayClass) {

        if (dimension < 2) {
            throw new IllegalArgumentException("dimension error");
        }
        final char delim = meta.dataType == PgType.BOX_ARRAY ? ';' : COMMA;

        final Stack<List<Object>> arrayStack = new FastStack<>();
        arrayStack.push(new LinkedList<>());
        char ch;
        for (int i = index, dimensionIndex = dimension; i < charArray.length; i++) {
            ch = charArray[i];
            if (ch != LEFT_PAREN) {
                if (!Character.isWhitespace(ch)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("ch:{},value:{}", ch, new String(charArray));
                    }
                    throw arrayFormatError(meta);
                }
                continue;
            }
            if (dimensionIndex > 1) {
                dimensionIndex--;
                if (dimension - arrayStack.size() > 1) {
                    arrayStack.push(new LinkedList<>());
                }
                continue;
            }
            if (arrayStack.size() >= dimension) {
                // here bug
                String m = String.format("array parse error,dimension[%s],arrayStack.size[%s]."
                        , dimension, arrayStack.size());
                throw new IllegalStateException(m);
            }
            // below read one dimension array
            final ArrayPair pair;
            pair = function.apply(charArray, i);
            i = pair.index;
            if (charArray[i] != RIGHT_PAREN) {
                throw new IllegalArgumentException(String.format("function[%s] error.", function));
            }
            // validate one dimension array
            final Pair<Class<?>, Integer> diPair = PgArrays.getPgArrayType(pair.array.getClass());
            if (diPair.getFirst() != targetArrayClass || diPair.getSecond() != 1) {
                throw new IllegalArgumentException(String.format("function[%s] error.", function));
            }

            // add one dimension array to list
            arrayStack.peek().add(pair.array);
            // handle text after one dimension
            for (int j = i + 1; j < charArray.length; j++) {
                ch = charArray[j];
                if (ch == delim) {
                    // has more element of two dimension array
                    i = j;
                    break;
                } else if (ch == RIGHT_PAREN) {
                    if (arrayStack.size() == 1) {
                        i = j;
                        break;
                    }
                    final List<Object> arrayList = arrayStack.pop();
                    final Object array;
                    array = createArray(++dimensionIndex, arrayList, targetArrayClass);
                    arrayStack.peek().add(array);
                } else if (ch == LEFT_PAREN) {
                    i = j - 1;
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
        return createArray(dimension, arrayStack.pop(), targetArrayClass);
    }


    /**
     * @return first: dimension of array,second:next index of charArray
     */
    private static Pair<Integer, Integer> getArrayDimension(final char[] charArray, final PgColumnMeta meta
            , final Class<?> targetArrayClass) {
        int i = 0;
        for (; i < charArray.length; i++) {
            if (!Character.isWhitespace(charArray[i])) {
                break;
            }
        }

        final Pair<Integer, Integer> dimensionPair;
        if (charArray[i] == '[') {
            final int[] index = new int[]{i};
            final List<Pair<Integer, Integer>> list = readArraySubScriptList(charArray, index, meta);
            if (charArray[index[0]] != '=') {
                throw arrayFormatError(meta);
            }
            for (Pair<Integer, Integer> pair : list) {
                if (pair.getFirst() < 1) {
                    throw notSupportSubScript(meta, targetArrayClass);
                }
            }
            dimensionPair = new Pair<>(list.size(), index[0] + 1);
        } else {
            int dimension = 0;
            for (; i < charArray.length; i++) {
                char c = charArray[i];
                if (c == '{') {
                    dimension++;
                } else if (!Character.isWhitespace(c)) {
                    break;
                }
            }
            dimensionPair = new Pair<>(dimension, 0);
        }

        if (dimensionPair.getFirst() < 1) {
            throw arrayFormatError(meta);
        }
        return dimensionPair;
    }

    private static List<Pair<Integer, Integer>> readArraySubScriptList(final char[] charArray, int[] index
            , final PgColumnMeta meta) {
        if (charArray[index[0]] != '[') {
            throw new IllegalArgumentException("index error");
        }

        final List<Pair<Integer, Integer>> list = new ArrayList<>();
        Pair<Integer, Integer> pair;

        for (int i = index[0]; i < charArray.length; i++) {
            if (Character.isWhitespace(charArray[i])) {
                continue;
            }
            if (charArray[i] == '=') {
                index[0] = i;
                break;
            }
            if (charArray[i] != '[') {
                throw arrayFormatError(meta);
            }
            index[0] = i;
            pair = readArraySubScript(charArray, index, meta);
            i = index[0];
            list.add(pair);

        }
        if (charArray[index[0]] != '=') {
            throw arrayFormatError(meta);
        }
        return Collections.unmodifiableList(list);
    }

    private static Pair<Integer, Integer> readArraySubScript(final char[] charArray, int[] index
            , final PgColumnMeta meta) {
        int i = index[0];
        if (charArray[i] != '[') {
            throw new IllegalArgumentException("i error");
        }
        int from = ++i;
        Integer lowerBound = null, upperBound = null;
        for (; i < charArray.length; i++) {
            if (charArray[i] == ':') {
                lowerBound = Integer.parseInt(new String(charArray, from, i));
                break;
            }
        }
        if (lowerBound == null) {
            throw arrayFormatError(meta);
        }
        for (from = ++i; i < charArray.length; i++) {
            if (charArray[i] == ']') {
                upperBound = Integer.parseInt(new String(charArray, from, i));
                break;
            }
        }
        if (upperBound == null) {
            throw arrayFormatError(meta);
        }
        index[i] = i;
        return new Pair<>(lowerBound, upperBound);
    }

    /**
     * @see #readMultiDimensionArray(int, char[], int, PgColumnMeta, BiFunction, Class)
     */
    private static Object createArray(final int dimension, final List<Object> valueList
            , final Class<?> targetArrayClass) {
        final int actualDimension;
        final Class<?> componentType;
        if (targetArrayClass == byte[].class) {
            componentType = byte.class;
            actualDimension = dimension + 1;
        } else {
            componentType = targetArrayClass;
            actualDimension = dimension;
        }
        final Object array;
        array = PgArrays.createArrayInstance(componentType, actualDimension, valueList.size());
        int index = 0;
        for (Object value : valueList) {
            try {
                Array.set(array, index, value);
            } catch (IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            index++;
        }
        return array;
    }

    /**
     * @see #readMultiDimensionArray(int, char[], int, PgColumnMeta, BiFunction, Class)
     */
    private static int readOneDimensionArray(final char[] charArray, final int index
            , final PgColumnMeta meta, final Consumer<String> consumer) throws IllegalArgumentException {
        if (charArray[index] != LEFT_PAREN) {
            throw new IllegalArgumentException("index error.");
        }
        final char delim = meta.dataType == PgType.BOX_ARRAY ? ';' : COMMA;
        int endIndex = index;
        char ch;
        for (int i = index + 1, from, to; i < charArray.length; i++) {
            ch = charArray[i];
            if (Character.isWhitespace(ch)) {
                continue;
            }
            from = i;
            to = -1;
            if (ch == DOUBLE_QUOTE) {
                from++;
                for (int j = from; j < charArray.length; j++) {
                    ch = charArray[j];
                    if (ch == BACKSLASH) {
                        j++;
                        continue;
                    }
                    if (ch == DOUBLE_QUOTE) {
                        to = j;
                        break;
                    }
                }
                if (to < 0) {
                    throw arrayFormatError(meta);
                }
            }
            for (int j = (to < 0 ? from + 1 : to + 1); j < charArray.length; j++) {
                ch = charArray[j];
                if (ch == delim || ch == RIGHT_PAREN) {
                    if (to < 0) {
                        to = j;
                    }
                    endIndex = j;
                    break;
                }
            }
            if (to < 0) {
                throw arrayFormatError(meta);
            }
            if (to - from == 4 && isNull(charArray, from)) {
                consumer.accept(null);
            } else if (charArray[to] == PgConstant.DOUBLE_QUOTE) {
                consumer.accept(escapesText(charArray, from, to, meta));
            } else {
                consumer.accept(new String(charArray, from, to - from));
            }
            if (charArray[endIndex] == RIGHT_PAREN) {
                break;
            }
            i = endIndex;
        }
        if (charArray[endIndex] != RIGHT_PAREN) {
            throw arrayFormatError(meta);
        }
        return endIndex;
    }

    /**
     * @see #readOneDimensionArray(char[], int, PgColumnMeta, Consumer)
     */
    private static String escapesText(final char[] charArray, final int from, final int to, final PgColumnMeta meta) {
        final StringBuilder builder = new StringBuilder(to - from);
        final int lastIndex = to - 1;
        int lastWritten = from;
        for (int i = from; i < to; i++) {
            if (charArray[i] != PgConstant.BACK_SLASH) {
                continue;
            }
            if (i == lastIndex) {
                throw arrayFormatError(meta);
            }
            if (i > lastWritten) {
                builder.append(charArray, lastWritten, i - lastWritten);
            }
            builder.append(charArray[i + 1]);
            lastWritten = i + 2;
            i++;

        }
        if (lastWritten < to) {
            builder.append(charArray, lastWritten, to - lastWritten);
        }
        return builder.toString();
    }


    private static boolean isNull(final char[] charArray, int from) {
        return (charArray[from] == 'N' || charArray[from] == 'n')
                && (charArray[++from] == 'U' || charArray[from] == 'u')
                && (charArray[++from] == 'L' || charArray[from] == 'l')
                && (charArray[++from] == 'L' || charArray[from] == 'l');
    }


    private static PgJdbdException arrayFormatError(final PgColumnMeta meta) {
        throw new JdbdException(String.format("Postgre server response %s value error,couldn't parse,ColumnMeta[%s]"
                , meta.dataType, meta));
    }

    private static IllegalArgumentException dateInfinityException(Object parsedValue, Class<?> arrayClass
            , PgColumnMeta meta) {
        String m;
        m = String.format("%s can't parse to %s,column label[%s]", parsedValue, arrayClass.getName(), meta.columnLabel);
        return new IllegalArgumentException(m);
    }


    private static IllegalArgumentException targetArrayClassError(final Class<?> targetArrayClass) {
        return new IllegalArgumentException(String.format("Error targetArrayClass[%s]", targetArrayClass.getName()));
    }

    static UnsupportedConvertingException notSupportSubScript(PgColumnMeta meta, Class<?> targetClass) {
        String message = String.format("Not support convert from (index[%s] label[%s] and sql type[%s]) to %s ,because subscript of array less than 1.",
                meta.columnIndex, meta.columnLabel
                , meta.dataType, targetClass.getName());

        return new UnsupportedConvertingException(message, meta.dataType, targetClass);
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
