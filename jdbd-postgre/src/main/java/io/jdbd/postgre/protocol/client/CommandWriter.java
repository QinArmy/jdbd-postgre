package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgArrays;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.type.Interval;
import io.jdbd.vendor.stmt.ParamValue;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.function.BiConsumer;

abstract class CommandWriter {

    final TaskAdjutant adjutant;

    final Charset clientCharset;

    final boolean clientUtf8;


    CommandWriter(final TaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.clientCharset = adjutant.clientCharset();
        this.clientUtf8 = this.clientCharset.equals(StandardCharsets.UTF_8);
    }


    final void writeArray(final int batchIndex, final ParamValue paramValue, final ByteBuf message) {
        final Object arrayValue = paramValue.getNonNullValue();
        final Class<?> componentType;
        componentType = PgArrays.underlyingComponent(arrayValue.getClass());

        final BiConsumer<Object, ByteBuf> consumer;
        final Charset clientCharset = this.clientCharset;
        final String v;
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
            case DOUBLE_ARRAY: {
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
                if (componentType != LocalDate.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeLocalDate;
            }
            break;
            case TIMESTAMP_ARRAY: {
                if (componentType != LocalDateTime.class) {
                    throw PgExceptions.nonSupportBindSqlTypeError(batchIndex, paramValue);
                }
                consumer = this::writeLocalDateTime;
            }
            break;
            case TIMESTAMPTZ_ARRAY: {
                if (componentType != OffsetDateTime.class) {
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
            case BYTEA_ARRAY:

            case BIT_ARRAY:
            case VARBIT_ARRAY:

            case UUID_ARRAY:

            case CHAR_ARRAY:
            case VARCHAR_ARRAY:
            case TEXT_ARRAY:
            case JSON_ARRAY:
            case JSONB_ARRAY:
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
            case MACADDR8_ARRAY:
                break;
            default:
                throw PgExceptions.unexpectedEnum(pgType);
        }
    }


    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeBooleanElement(final Object element, final ByteBuf message) {
        if ((Boolean) element) {
            message.writeBytes(PgConstant.TRUE.getBytes(this.clientCharset));
        } else {
            message.writeBytes(PgConstant.FALSE.getBytes(this.clientCharset));
        }
    }

    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeShortElement(final Object element, final ByteBuf message) {
        message.writeBytes(Short.toString((Short) element).getBytes(this.clientCharset));
    }

    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeIntegerElement(final Object element, final ByteBuf message) {
        message.writeBytes(Integer.toString((Integer) element).getBytes(this.clientCharset));
    }

    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeLongElement(final Object element, final ByteBuf message) {
        message.writeBytes(Long.toString((Long) element).getBytes(this.clientCharset));
    }

    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeBigDecimalElement(final Object element, final ByteBuf message) {
        message.writeBytes(((BigDecimal) element).toPlainString().getBytes(this.clientCharset));
    }

    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeFloatElement(final Object element, final ByteBuf message) {
        message.writeBytes(((Float) element).toString().getBytes(this.clientCharset));
    }

    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeDoubleElement(final Object element, final ByteBuf message) {
        message.writeBytes(((Double) element).toString().getBytes(this.clientCharset));
    }


    /**
     * @see #writeArray(int, ParamValue, ByteBuf)
     */
    private void writeStringElement(final Object element, final ByteBuf message) {
        //TODO
        throw new UnsupportedOperationException();
    }

    private void writeLocalTime(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(((LocalTime) element).format(PgTimes.TIME_FORMATTER_6).getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    private void writeOffsetTime(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(((OffsetTime) element).format(PgTimes.OFFSET_TIME_FORMATTER_6).getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    private void writeLocalDate(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(((LocalDate) element).toString().getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }

    private void writeLocalDateTime(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(
                ((LocalDateTime) element).format(PgTimes.DATETIME_FORMATTER_6).getBytes(this.clientCharset)
        );
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }


    private void writeOffsetDateTime(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(
                ((OffsetDateTime) element).format(PgTimes.OFFSET_DATETIME_FORMATTER_6).getBytes(this.clientCharset)
        );
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }


    private void writeIntervalElement(final Object element, final ByteBuf message) {
        message.writeByte(PgConstant.DOUBLE_QUOTE);
        message.writeBytes(((Interval) element).toString(true).getBytes(this.clientCharset));
        message.writeByte(PgConstant.DOUBLE_QUOTE);
    }


}
