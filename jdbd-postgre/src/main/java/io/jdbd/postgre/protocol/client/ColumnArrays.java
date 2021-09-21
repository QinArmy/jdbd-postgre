package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.type.PgBox;
import io.jdbd.postgre.type.PgGeometries;
import io.jdbd.postgre.type.PgLine;
import io.jdbd.postgre.type.PgPolygon;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgBuffers;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.type.Interval;
import io.jdbd.type.LongBinary;
import io.jdbd.type.geo.Line;
import io.jdbd.type.geo.LineString;
import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.LongString;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import org.qinarmy.util.FastStack;
import org.qinarmy.util.Pair;
import org.qinarmy.util.Stack;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.time.*;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;

abstract class ColumnArrays {

    private ColumnArrays() {
        throw new UnsupportedOperationException();
    }

    private static final char LEFT_PAREN = '{';
    private static final char COMMA = ',';
    private static final char RIGHT_PAREN = '}';
    private static final char DOUBLE_QUOTE = '"';
    private static final char BACKSLASH = '\\';

    static Object readBooleanArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Boolean> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(Boolean.parseBoolean(nullable));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Boolean[0]), endIndex);
        };
        return readArray(value, meta, function, Boolean.class);
    }


    static Object readShortArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Short> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(Short.parseShort(nullable));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Short[0]), endIndex);
        };
        return readArray(value, meta, function, Short.class);
    }

    static Object readIntegerArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Integer> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(Integer.parseInt(nullable));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Integer[0]), endIndex);
        };
        return readArray(value, meta, function, Integer.class);
    }

    static Object readBigIntArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Long> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(Long.parseLong(nullable));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Long[0]), endIndex);
        };
        return readArray(value, meta, function, Long.class);
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
                if (nullable.equalsIgnoreCase("NaN")) {
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

    static Object readRealArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Float> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(Float.parseFloat(nullable));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Float[0]), endIndex);
        };
        return readArray(value, meta, function, Float.class);
    }

    static Object readDoubleArray(final String value, final PgColumnMeta meta) {
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Double> list = new LinkedList<>();
            final int endIndex;
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(Double.parseDouble(nullable));
                }
            };
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Double[0]), endIndex);
        };
        return readArray(value, meta, function, Double.class);
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
                    list.add(nullable);
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
                    list.add(nullable);
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
                } else if (targetArrayClass == Object.class) {
                    list.add(parsedValue);
                } else if (targetArrayClass == String.class) {
                    list.add(nullable);
                } else {
                    throw targetArrayClassError(targetArrayClass);
                }

            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == OffsetDateTime.class) {
                array = new OffsetDateTime[list.size()];
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
        if (targetArrayClass != BitSet.class && targetArrayClass != Long.class && targetArrayClass != Integer.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Object> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                    return;
                }
                if (targetArrayClass == BitSet.class) {
                    list.add(PgStrings.bitStringToBitSet(nullable, false));
                } else if (targetArrayClass == Long.class) {
                    final String v = new StringBuilder(nullable).reverse().toString();
                    list.add(Long.parseLong(v, 2));
                } else {
                    final String v = new StringBuilder(nullable).reverse().toString();
                    list.add(Integer.parseInt(v, 2));
                }

            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == BitSet.class) {
                array = new BitSet[list.size()];
            } else if (targetArrayClass == Long.class) {
                array = new Long[list.size()];
            } else {
                array = new Integer[list.size()];
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    /**
     * @throws io.jdbd.result.UnsupportedConvertingException when targetArrayClass and value not match.
     */
    static Object readMoneyArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass
            , final DecimalFormat format) {
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
        if (targetArrayClass != byte[].class && targetArrayClass != LongBinary.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
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
                } else {
                    list.add(LongBinaries.fromArray(bytes));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == byte[].class) {
                array = new byte[list.size()][];
            } else {
                array = new LongBinary[list.size()];
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readTextArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != String.class && targetArrayClass != LongString.class) {
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
                    list.add(LongStrings.fromString(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == String.class) {
                array = new String[list.size()];
            } else {
                array = new LongString[list.size()];
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readIntervalArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Interval.class
                && targetArrayClass != Duration.class
                && targetArrayClass != Period.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
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
                    list.add(v.toDurationExact());
                } else {
                    list.add(v.toPeriodExact());
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            final Object[] array;
            if (targetArrayClass == Interval.class) {
                array = new Interval[list.size()];
            } else if (targetArrayClass == Duration.class) {
                array = new Duration[list.size()];
            } else {
                array = new Period[list.size()];
            }
            return new ArrayPair(list.toArray(array), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }


    static Object readPointArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Point.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Point> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(PgGeometries.point(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Point[0]), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readLineArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != PgLine.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<PgLine> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(PgGeometries.line(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new PgLine[0]), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readLineSegmentArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Line.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Line> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(PgGeometries.lineSegment(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Line[0]), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readBoxArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != PgBox.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<PgBox> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(PgGeometries.box(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new PgBox[0]), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readPathArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != LineString.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<LineString> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(PgGeometries.path(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new LineString[0]), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readPolygonArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != PgPolygon.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<PgPolygon> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(PgGeometries.polygon(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new PgPolygon[0]), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    static Object readCirclesArray(final String value, final PgColumnMeta meta, final Class<?> targetArrayClass) {
        if (targetArrayClass != Circle.class) {
            throw PgResultRow.notSupportConverting(meta, targetArrayClass);
        }
        final BiFunction<char[], Integer, ArrayPair> function = (charArray, index) -> {
            final List<Circle> list = new LinkedList<>();
            final Consumer<String> consumer = nullable -> {
                if (nullable == null) {
                    list.add(null);
                } else {
                    list.add(PgGeometries.circle(nullable));
                }
            };
            final int endIndex;
            endIndex = readOneDimensionArray(charArray, index, meta, consumer);
            return new ArrayPair(list.toArray(new Circle[0]), endIndex);
        };
        return readArray(value, meta, function, targetArrayClass);
    }

    private static Object readArray(final String value, final PgColumnMeta meta
            , final BiFunction<char[], Integer, ArrayPair> function, final Class<?> targetArrayClass) {
        final char[] charArray = value.toCharArray();
        //1. parse array dimension.
        final int dimension;
        dimension = getArrayDimension(charArray, meta);
        //2. parse array
        final Object array;
        if (dimension > 1) {
            array = readMultiDimensionArray(dimension, charArray, meta, function, targetArrayClass);
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
            , final PgColumnMeta meta, final BiFunction<char[], Integer, ArrayPair> function
            , final Class<?> targetArrayClass) {
        if (dimension < 2) {
            throw new IllegalArgumentException("dimension error");
        }
        final PgType elementType = Objects.requireNonNull(meta.sqlType.elementType(), "elementType");

        assertTargetArrayClass(targetArrayClass, elementType);

        final Stack<List<Object>> arrayStack = new FastStack<>();
        arrayStack.push(new LinkedList<>());
        char ch;
        for (int i = 0, dimensionIndex = dimension; i < charArray.length; i++) {
            ch = charArray[i];
            if (ch != LEFT_PAREN) {
                if (!Character.isWhitespace(ch)) {
                    throw arrayFormatError(meta);
                }
                continue;
            }
            dimensionIndex--;
            if (dimensionIndex > 1) {
                arrayStack.push(new LinkedList<>());
                if (dimensionIndex != arrayStack.size()) {
                    // here bug
                    String m = String.format("array parse error,dimensionIndex[%s],arrayStack.size[%s]."
                            , dimensionIndex, arrayStack.size());
                    throw new IllegalStateException(m);
                }
                continue;
            }
            // below read one dimension array
            final ArrayPair pair;
            pair = function.apply(charArray, i);
            dimensionIndex++;
            i = pair.index;
            if (charArray[i] != RIGHT_PAREN) {
                throw new IllegalArgumentException(String.format("function[%s] error.", function));
            }
            // validate one dimension array
            final Class<?> arrayCass = pair.array.getClass();
            final Pair<Class<?>, Integer> diPair = PgBinds.getArrayDimensions(arrayCass);
            if (!arrayCass.isArray() || diPair.getFirst() != targetArrayClass || diPair.getSecond() != 1) {
                throw new IllegalArgumentException(String.format("function[%s] error.", function));
            }
            // add one dimension array to list
            arrayStack.peek().add(pair.array);
            // handle text after one dimension
            for (int j = i + 1; j < charArray.length; j++) {
                ch = charArray[j];
                if (ch == COMMA) {
                    // has more element of two dimension array
                    i = j;
                    break;
                } else if (ch == RIGHT_PAREN) {
                    if (arrayStack.size() < 2) {
                        i = j;
                        break;
                    }
                    final List<Object> arrayList = arrayStack.pop();
                    final Object array;
                    array = createArray(dimensionIndex, arrayList, targetArrayClass);
                    arrayStack.peek().add(array);
                    dimensionIndex++;
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


    private static Object createArray(final int dimension, final List<Object> valueList
            , final Class<?> targetArrayClass) {

        final String className;
        if (targetArrayClass.isPrimitive()) {
            if (targetArrayClass == int.class) {
                className = "I";
            } else if (targetArrayClass == long.class) {
                className = "J";
            } else if (targetArrayClass == short.class) {
                className = "S";
            } else if (targetArrayClass == byte.class) {
                className = "B";
            } else if (targetArrayClass == boolean.class) {
                className = "Z";
            } else if (targetArrayClass == float.class) {
                className = "F";
            } else if (targetArrayClass == double.class) {
                className = "D";
            } else {
                String m = String.format("targetArrayClass[%s] not supported", targetArrayClass.getName());
                throw new IllegalArgumentException(m);
            }
        } else {
            className = targetArrayClass.getName();
        }

        final StringBuilder builder = new StringBuilder(dimension + 2 + className.length());


        for (int i = 0; i < dimension; i++) {
            builder.append('[');
        }
        final boolean appendL = targetArrayClass != byte[].class && !targetArrayClass.isPrimitive();
        if (appendL) {
            builder.append('L');
        }
        builder.append(className);
        if (appendL) {
            builder.append(';');
        }
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


    private static int readOneDimensionArray(final char[] charArray, final int index
            , final PgColumnMeta meta, final Consumer<String> consumer) throws IllegalArgumentException {
        if (charArray[index] != LEFT_PAREN) {
            throw new IllegalArgumentException("index error.");
        }
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
                if (ch == COMMA || ch == RIGHT_PAREN) {
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
            } else {
                consumer.accept(new String(charArray, from, to - from));
            }
            if (charArray[endIndex] == RIGHT_PAREN) {
                break;
            }
        }
        if (charArray[endIndex] != RIGHT_PAREN) {
            throw arrayFormatError(meta);
        }
        return endIndex;
    }


    private static boolean isNull(final char[] charArray, int from) {
        return (charArray[from] == 'N' || charArray[from] == 'n')
                && (charArray[++from] == 'U' || charArray[from] == 'u')
                && (charArray[++from] == 'L' || charArray[from] == 'l')
                && (charArray[++from] == 'L' || charArray[from] == 'l');
    }


    private static PgJdbdException arrayFormatError(final PgColumnMeta meta) {
        throw new PgJdbdException(String.format("Postgre server response %s value error,couldn't parse,ColumnMeta[%s]"
                , meta.sqlType, meta));
    }

    private static IllegalArgumentException dateInfinityException(Object parsedValue, Class<?> arrayClass
            , PgColumnMeta meta) {
        String m;
        m = String.format("%s can't parse to %s,column label[%s]", parsedValue, arrayClass.getName(), meta.columnLabel);
        return new IllegalArgumentException(m);
    }

    private static void assertTargetArrayClass(final Class<?> targetArrayClass, final PgType elementType) {
        if (targetArrayClass != elementType.javaType()
                && targetArrayClass != Object.class
                && targetArrayClass != String.class) {
            throw targetArrayClassError(targetArrayClass);
        }
    }

    private static IllegalArgumentException targetArrayClassError(final Class<?> targetArrayClass) {
        return new IllegalArgumentException(String.format("Error targetArrayClass[%s]", targetArrayClass.getName()));
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
