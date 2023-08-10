package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.*;
import io.jdbd.type.Interval;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.BitSet;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link QueryCommandWriter}</li>
 *         <li>{@link PgExtendedCommandWriter}</li>
 *     </ul>
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype.html">Data Types</a>
 * @see <a href="https://www.postgresql.org/docs/current/arrays.html">Arrays Types</a>
 * @since 1.0
 */
abstract class CommandWriter {

    final TaskAdjutant adjutant;

    final Charset clientCharset;

    final boolean clientUtf8;


    CommandWriter(final TaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.clientCharset = adjutant.clientCharset();
        this.clientUtf8 = this.clientCharset.equals(StandardCharsets.UTF_8);
    }


    /**
     * @return the dimension of array.
     * @see <a href="https://www.postgresql.org/docs/current/datatype.html">Data Types</a>
     * @see <a href="https://www.postgresql.org/docs/current/arrays.html">Arrays Types</a>
     */
    final int writeArrayObject(final int batchIndex, final ParamValue paramValue, final ByteBuf message) {
        final Object arrayValue = paramValue.getNonNullValue();
        final Class<?> arrayClass = arrayValue.getClass();
        final Class<?> componentType;
        componentType = PgArrays.underlyingComponent(arrayClass);

        final int dimension;
        dimension = PgArrays.dimensionOf(arrayClass);

        final BiConsumer<Object, ByteBuf> consumer;
        final PgType pgType = (PgType) paramValue.getType();

        switch (pgType) {
            case BOOLEAN_ARRAY: {
                if (componentType != Boolean.class && componentType != boolean.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeBooleanElement;
            }
            break;
            case SMALLINT_ARRAY: {
                if (componentType != Short.class && componentType != short.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeShortElement;
            }
            break;
            case INTEGER_ARRAY: {
                if (componentType != Integer.class && componentType != int.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeIntegerElement;
            }
            break;
            case OID_ARRAY:
            case BIGINT_ARRAY: {
                if (componentType != Long.class && componentType != long.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeLongElement;
            }
            break;
            case DECIMAL_ARRAY: {
                if (componentType != BigDecimal.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeBigDecimalElement;
            }
            break;
            case REAL_ARRAY: {
                if (componentType != Float.class && componentType != float.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeFloatElement;
            }
            break;
            case FLOAT8_ARRAY: {
                if (componentType != Double.class && componentType != double.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeDoubleElement;
            }
            break;
            case MONEY_ARRAY: {
                if (componentType == BigDecimal.class) {
                    consumer = this::writeBigDecimalElement;
                } else if (componentType == String.class) {
                    consumer = this::writeStringElement;
                } else {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
            }
            break;
            case TIME_ARRAY: {
                if (componentType != LocalTime.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeLocalTime;
            }
            break;
            case TIMETZ_ARRAY: {
                if (componentType != OffsetTime.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeOffsetTime;
            }
            break;
            case DATE_ARRAY: {
                if (componentType != LocalDate.class
                        && componentType != String.class
                        && componentType != Object.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeLocalDate;
            }
            break;
            case TIMESTAMP_ARRAY: {
                if (componentType != LocalDateTime.class
                        && componentType != String.class
                        && componentType != Object.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeLocalDateTime;
            }
            break;
            case TIMESTAMPTZ_ARRAY: {
                if (componentType != OffsetDateTime.class
                        && componentType != String.class
                        && componentType != Object.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeOffsetDateTime;
            }
            break;
            case INTERVAL_ARRAY: {
                if (componentType == Interval.class) {
                    consumer = this::writeIntervalElement;
                } else if (componentType == String.class) {
                    consumer = this::writeStringElement;
                } else {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
            }
            break;
            case BYTEA_ARRAY: {
                if (componentType != byte.class || dimension < 2) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeByteaElement;
            }
            break;
            case BIT_ARRAY:
            case VARBIT_ARRAY: {
                if (componentType != Integer.class
                        && componentType != Long.class
                        && componentType != String.class
                        && componentType != BitSet.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeBitElement;
            }
            break;
            case UUID_ARRAY: {
                if (componentType != UUID.class && componentType != String.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeUuidElement;
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

            case POINT_ARRAY:
            case LINE_ARRAY:
            case PATH_ARRAY:
            case BOX_ARRAY:
            case LSEG_ARRAY:
            case CIRCLE_ARRAY:
            case POLYGON_ARRAY:

            case CIDR_ARRAY:
            case INET_ARRAY:
            case MACADDR_ARRAY:
            case MACADDR8_ARRAY: {
                if (componentType != String.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeStringElement;
            }
            break;
            default:
                throw PgExceptions.unexpectedEnum(pgType);
        }


        final boolean oneDimension;
        if (pgType == PgType.BYTEA_ARRAY) {
            oneDimension = dimension == 2;
        } else {
            oneDimension = dimension == 1;
        }

        switch (pgType) {
            case DATE_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
                // no-op
                break;
            default: {
                if (componentType == String.class) {
                    message.writeByte('E');
                }
            }
        }
        message.writeByte(PgConstant.QUOTE);
        if (oneDimension) {
            writeOneDimensionArray((Object[]) arrayValue, pgType, consumer, message);
        } else {
            writeMultiDimensionArray(arrayValue, pgType, consumer, message);
        }
        message.writeByte(PgConstant.QUOTE);

        return pgType == PgType.BYTEA_ARRAY ? (dimension - 1) : dimension;
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeOneDimensionArray(final Object[] array, final PgType pgType,
                                        final BiConsumer<Object, ByteBuf> consumer, final ByteBuf message) {
        final int length = array.length;

        message.writeByte(PgConstant.LEFT_BRACE);
        Object element;
        byte[] nullBytes = null;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                if (pgType == PgType.BOX_ARRAY) {
                    message.writeByte(PgConstant.SEMICOLON);
                } else {
                    message.writeByte(PgConstant.COMMA);
                }
            }
            element = array[i];
            if (element == null) {
                if (nullBytes == null) {
                    nullBytes = PgConstant.NULL.getBytes(this.clientCharset);
                }
                message.writeBytes(nullBytes);
            } else {
                consumer.accept(element, message);
            }

        }
        message.writeByte(PgConstant.RIGHT_BRACE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeMultiDimensionArray(final Object array, final PgType pgType,
                                          final BiConsumer<Object, ByteBuf> consumer, final ByteBuf message) {
        final int length, dimension;
        length = Array.getLength(array);
        dimension = PgArrays.dimensionOf(array.getClass());

        final boolean outElement;
        if (pgType == PgType.BYTEA_ARRAY) {
            outElement = dimension == 2;
        } else {
            outElement = dimension == 1;
        }

        message.writeByte(PgConstant.LEFT_BRACE);
        Object element;
        byte[] nullBytes = null;
        for (int i = 0; i < length; i++) {
            if (i > 0) {
                if (pgType == PgType.BOX_ARRAY) {
                    message.writeByte(PgConstant.SEMICOLON);
                } else {
                    message.writeByte(PgConstant.COMMA);
                }
            }

            element = Array.get(array, i);
            if (element == null) {
                if (nullBytes == null) {
                    nullBytes = PgConstant.NULL.getBytes(this.clientCharset);
                }
                message.writeBytes(nullBytes);
            } else if (outElement) {
                consumer.accept(element, message);
            } else {
                writeMultiDimensionArray(element, pgType, consumer, message);
            }

        }
        message.writeByte(PgConstant.RIGHT_BRACE);

    }


    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeBooleanElement(final Object element, final ByteBuf message) {
        if ((Boolean) element) {
            message.writeBytes(PgConstant.TRUE.getBytes(this.clientCharset));
        } else {
            message.writeBytes(PgConstant.FALSE.getBytes(this.clientCharset));
        }
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeShortElement(final Object element, final ByteBuf message) {
        message.writeBytes(Short.toString((Short) element).getBytes(this.clientCharset));
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeIntegerElement(final Object element, final ByteBuf message) {
        message.writeBytes(Integer.toString((Integer) element).getBytes(this.clientCharset));
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeLongElement(final Object element, final ByteBuf message) {
        message.writeBytes(Long.toString((Long) element).getBytes(this.clientCharset));
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeBigDecimalElement(final Object element, final ByteBuf message) {
        message.writeBytes(((BigDecimal) element).toPlainString().getBytes(this.clientCharset));
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeFloatElement(final Object element, final ByteBuf message) {
        message.writeBytes(((Float) element).toString().getBytes(this.clientCharset));
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeDoubleElement(final Object element, final ByteBuf message) {
        message.writeBytes(((Double) element).toString().getBytes(this.clientCharset));
    }


    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeStringElement(final Object element, final ByteBuf message) {
        final byte[] bytes;
        bytes = ((String) element).getBytes(this.clientCharset);
        final int length = bytes.length;

        message.writeByte(PgConstant.DOUBLE_QUOTE);

        int lastWritten = 0;
        char followChar;
        for (int i = 0, byteValue; i < length; i++) {
            byteValue = bytes[i];
            switch (byteValue) {
                case PgConstant.QUOTE: {
                    if (i > lastWritten) {
                        message.writeBytes(bytes, lastWritten, i - lastWritten);
                    }
                    message.writeByte(PgConstant.QUOTE);  // because jdbd-postgre support only the charset that ASCII is one byte
                    lastWritten = i;//not i + 1 as current char wasn't written
                }
                continue;
                case PgConstant.BACK_SLASH:
                    followChar = PgConstant.BACK_SLASH;
                    break;
                case PgConstant.DOUBLE_QUOTE:
                    followChar = PgConstant.DOUBLE_QUOTE;
                    break;
                case PgConstant.NUL:
                    followChar = '0';
                    break;
                case '\b':
                    followChar = 'b';
                    break;
                case '\f':
                    followChar = 'f';
                    break;
                case '\n':
                    followChar = 'n';
                    break;
                case '\r':
                    followChar = 'r';
                    break;
                case '\t':
                    followChar = 't';
                    break;
                default:
                    continue;
            }

            if (i > lastWritten) {
                message.writeBytes(bytes, lastWritten, i - lastWritten);
            }
            message.writeByte(PgConstant.BACK_SLASH);  // because jdbd-postgre support only the charset that ASCII is one byte
            message.writeByte(followChar);
            lastWritten = i + 1;


        }// for

        if (lastWritten < length) {
            message.writeBytes(bytes, lastWritten, length - lastWritten);
        }
        message.writeByte(PgConstant.DOUBLE_QUOTE);

    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeLocalTime(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(((LocalTime) element).format(PgTimes.TIME_FORMATTER_6).getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeOffsetTime(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(((OffsetTime) element).format(PgTimes.OFFSET_TIME_FORMATTER_6).getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-DATE-TABLE">Date Input</a>
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-TABLE">Special Values</a>
     */
    private void writeLocalDate(final Object element, final ByteBuf message) {
        final String value;
        if (element instanceof LocalDate) {
            value = element.toString();
        } else if (!(element instanceof String)) {
            String m = String.format("%s don't support element javaType[%s]", PgType.DATE_ARRAY,
                    PgClasses.safeClassName(element));
            throw new JdbdException(m);
        } else if (PgConstant.INFINITY.equalsIgnoreCase((String) element)) {
            value = PgConstant.INFINITY;
        } else if (PgConstant.NEG_INFINITY.equalsIgnoreCase((String) element)) {
            value = PgConstant.NEG_INFINITY;
        } else {
            String m = String.format("%s don't support element %s", PgType.DATE_ARRAY, element);
            throw new JdbdException(m);
        }

        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(value.getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-TABLE">Special Values</a>
     */
    private void writeLocalDateTime(final Object element, final ByteBuf message) {
        final String value;
        if (element instanceof LocalDateTime) {
            value = ((LocalDateTime) element).format(PgTimes.DATETIME_FORMATTER_6);
        } else if (!(element instanceof String)) {
            String m = String.format("%s don't support element javaType[%s]", PgType.TIMESTAMP_ARRAY,
                    PgClasses.safeClassName(element));
            throw new JdbdException(m);
        } else if (PgConstant.INFINITY.equalsIgnoreCase((String) element)) {
            value = PgConstant.INFINITY;
        } else if (PgConstant.NEG_INFINITY.equalsIgnoreCase((String) element)) {
            value = PgConstant.NEG_INFINITY;
        } else {
            String m = String.format("%s don't support element %s", PgType.TIMESTAMP_ARRAY, element);
            throw new JdbdException(m);
        }

        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(value.getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     * @see <a href="https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-TABLE">Special Values</a>
     */
    private void writeOffsetDateTime(final Object element, final ByteBuf message) {
        final String value;
        if (element instanceof OffsetDateTime) {
            value = ((OffsetDateTime) element).format(PgTimes.OFFSET_DATETIME_FORMATTER_6);
        } else if (!(element instanceof String)) {
            String m = String.format("%s don't support element javaType[%s]", PgType.TIMESTAMPTZ_ARRAY,
                    PgClasses.safeClassName(element));
            throw new JdbdException(m);
        } else if (PgConstant.INFINITY.equalsIgnoreCase((String) element)) {
            value = PgConstant.INFINITY;
        } else if (PgConstant.NEG_INFINITY.equalsIgnoreCase((String) element)) {
            value = PgConstant.NEG_INFINITY;
        } else {
            String m = String.format("%s don't support element %s", PgType.TIMESTAMPTZ_ARRAY, element);
            throw new JdbdException(m);
        }

        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(value.getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeIntervalElement(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(((Interval) element).toString(true).getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     * @see <a href="https://www.postgresql.org/docs/current/datatype-binary.html">Binary Data Types</a>
     */
    private void writeByteaElement(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeByte(PgConstant.BACK_SLASH);
        message.writeByte('x');

        final byte[] bytea = (byte[]) element;
        message.writeBytes(PgBuffers.hexEscapes(true, bytea, bytea.length));

        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeBitElement(final Object element, final ByteBuf message) {
        final String bitString;
        if (element instanceof Integer) {
            bitString = Integer.toBinaryString((Integer) element);
        } else if (element instanceof Long) {
            bitString = Long.toBinaryString((Long) element);
        } else if (element instanceof BitSet) {
            bitString = PgStrings.bitSetToBitString((BitSet) element, true);
        } else if (!(element instanceof String)) {
            // no bug,never here
            throw new IllegalArgumentException();
        } else if (PgStrings.isBinaryString((String) element)) {
            bitString = (String) element;
        } else {
            throw new IllegalArgumentException("non binary string");
        }

        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(bitString.getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);

    }

    /**
     * @see #writeArrayObject(int, ParamValue, ByteBuf)
     */
    private void writeUuidElement(final Object element, final ByteBuf message) {
        final String uuidString;
        if (element instanceof UUID) {
            uuidString = element.toString();
        } else if (element instanceof String) {
            uuidString = (String) element;
        } else {
            // no bug,never here
            throw new IllegalArgumentException();
        }
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(uuidString.getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }


}
