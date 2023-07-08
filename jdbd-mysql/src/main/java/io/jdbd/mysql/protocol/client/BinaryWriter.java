package io.jdbd.mysql.protocol.client;


import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.vendor.stmt.Value;
import io.jdbd.vendor.util.JdbdExceptions;
import io.jdbd.vendor.util.JdbdTimes;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.temporal.ChronoField;
import java.util.BitSet;
import java.util.Set;

/**
 * <p>
 * This util class provider method that write MySQL Binary Protocol.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html">Binary Protocol Resultset</a>
 * @since 1.0
 */
abstract class BinaryWriter {

    private BinaryWriter() {
        throw new UnsupportedOperationException();
    }


    /**
     * <p>
     * Bind non-null value with MySQL binary protocol.
     * </p>
     *
     * @param precision  negative if dont' need to truncate micro seconds.
     * @param charset    client charset
     * @param serverZone null if as of MySQL 8.0.19 and {@link Value#getType()} is {@link MySQLType#DATETIME} or {@link MySQLType#TIMESTAMP} ,
     *                   now binary protocol parameter type is varchar
     *                   when {@link Value#getNonNullValue()} is following type:
     *                   <ul>
     *                      <li>{@link OffsetDateTime}</li>
     *                      <li>{@link ZonedDateTime}</li>
     *                   </ul>  see {@link #decideActualType(MySQLType, Value)}.
     *                   But if  {@link Value#getType()} is {@link MySQLType#TIME} serverZone must non-null.
     * @see #decideActualType(MySQLType, Value)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html">Binary Protocol Resultset</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">COM_STMT_EXECUTE</a>
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">COM_QUERY CLIENT_QUERY_ATTRIBUTES</a>
     */
    @SuppressWarnings("deprecation")
    static void writeBinary(final ByteBuf packet, final int batchIndex, final Value paramValue,
                            final int precision, final Charset charset, final @Nullable ZoneOffset serverZone) {

        switch ((MySQLType) paramValue.getType()) {
            case BOOLEAN: {
                final boolean v = MySQLBinds.bindToBoolean(batchIndex, paramValue);
                packet.writeByte(v ? 1 : 0);
            }
            break;
            case TINYINT:
                packet.writeByte(MySQLBinds.bindToInt(batchIndex, paramValue, Byte.MIN_VALUE, Byte.MAX_VALUE));
                break;
            case TINYINT_UNSIGNED:
                packet.writeByte(MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, 0xFF));
                break;
            case SMALLINT:
                Packets.writeInt2(packet, MySQLBinds.bindToInt(batchIndex, paramValue, Short.MIN_VALUE, Short.MAX_VALUE));
                break;
            case SMALLINT_UNSIGNED:
                Packets.writeInt2(packet, MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, 0xFFFF));
                break;
            case MEDIUMINT:
                Packets.writeInt3(packet, MySQLBinds.bindToInt(batchIndex, paramValue, 0x8000_00, 0X7FFF_FF));
                break;
            case MEDIUMINT_UNSIGNED:
                Packets.writeInt3(packet, MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, 0xFFFF_FF));
                break;
            case INT:
                Packets.writeInt4(packet, MySQLBinds.bindToInt(batchIndex, paramValue, Integer.MIN_VALUE, Integer.MAX_VALUE));
                break;
            case INT_UNSIGNED:
                Packets.writeInt4(packet, MySQLBinds.bindToIntUnsigned(batchIndex, paramValue, -1));
                break;
            case BIGINT:
                Packets.writeInt8(packet, MySQLBinds.bindToLong(batchIndex, paramValue, Long.MIN_VALUE, Long.MAX_VALUE));
                break;
            case BIGINT_UNSIGNED:
                Packets.writeInt8(packet, MySQLBinds.bindToLongUnsigned(batchIndex, paramValue, -1L));
                break;
            case YEAR:
                Packets.writeInt2(packet, MySQLBinds.bindToYear(batchIndex, paramValue));
                break;
            case DECIMAL: {
                final BigDecimal value;
                value = MySQLBinds.bindToDecimal(batchIndex, paramValue);
                Packets.writeStringLenEnc(packet, value.toPlainString().getBytes(charset));
            }
            break;
            case DECIMAL_UNSIGNED: {
                final BigDecimal value;
                value = MySQLBinds.bindToDecimal(batchIndex, paramValue);
                if (value.compareTo(BigDecimal.ZERO) < 0) {
                    throw MySQLExceptions.outOfTypeRange(batchIndex, paramValue, null);
                }
                Packets.writeStringLenEnc(packet, value.toPlainString().getBytes(charset));
            }
            break;
            case FLOAT: {
                final float value;
                value = MySQLBinds.bindToFloat(batchIndex, paramValue);
                Packets.writeInt4(packet, Float.floatToIntBits(value)); // here string[4] is Little Endian int4
            }
            break;
            case FLOAT_UNSIGNED: {
                final float value;
                value = MySQLBinds.bindToFloat(batchIndex, paramValue);
                if (value < 0.0) {
                    throw MySQLExceptions.outOfTypeRange(batchIndex, paramValue, null);
                }
                Packets.writeInt4(packet, Float.floatToIntBits(value));// here string[4] is Little Endian int4
            }
            break;
            case DOUBLE: {
                final double value;
                value = MySQLBinds.bindToDouble(batchIndex, paramValue);
                Packets.writeInt8(packet, Double.doubleToLongBits(value));// here string[8] is Little Endian int8
            }
            break;
            case DOUBLE_UNSIGNED: {
                final double value;
                value = MySQLBinds.bindToDouble(batchIndex, paramValue);
                if (value < 0.0d) {
                    throw MySQLExceptions.outOfTypeRange(batchIndex, paramValue, null);
                }
                Packets.writeInt8(packet, Double.doubleToLongBits(value));// here string[8] is Little Endian int8
            }
            break;
            case VARCHAR: {
                if (paramValue.getNonNull() instanceof Set) {
                    // Server response parameter metadata no MYSQL_TYPE_SET ,it's MYSQL_TYPE_VARCHAR
                    final String value;
                    value = MySQLBinds.bindToSetType(batchIndex, paramValue);
                    Packets.writeStringLenEnc(packet, value.getBytes(charset));
                } else {
                    writeString(packet, batchIndex, paramValue, charset);
                }
            }
            break;
            case ENUM:
            case CHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
            case JSON:
                writeString(packet, batchIndex, paramValue, charset);
                break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB: {
                final Object nonNull = paramValue.getNonNullValue();
                if (nonNull instanceof byte[]) {
                    Packets.writeStringLenEnc(packet, (byte[]) nonNull);
                } else { //TODO long data
                    throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
                }
            }
            break;
            case TIMESTAMP:
            case DATETIME:
                writeDatetime(packet, batchIndex, paramValue, precision, charset, serverZone);
                break;
            case TIME: {
                if (serverZone == null) {
                    // no bug never here
                    throw new NullPointerException("serverZone is null");
                }
                writeTime(packet, batchIndex, paramValue, precision, serverZone);
            }
            break;
            case DATE: {
                final LocalDate value;
                value = MySQLBinds.bindToLocalDate(batchIndex, paramValue);
                // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date
                packet.writeByte(4); // length
                Packets.writeInt2(packet, value.getYear());// year
                packet.writeByte(value.getMonthValue());// month
                packet.writeByte(value.getDayOfMonth());// day
            }
            break;
            case GEOMETRY: {
                final Object nonNull = paramValue.getNonNullValue();
                if (nonNull instanceof byte[]) {
                    Packets.writeStringLenEnc(packet, (byte[]) nonNull);
                } else if (nonNull instanceof String) {
                    Packets.writeStringLenEnc(packet, ((String) nonNull).getBytes(charset));
                } else {
                    throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
                }
            }
            break;
            case BIT:
                writeBit(packet, batchIndex, paramValue);
                break;
            case SET: {
                final String value;
                value = MySQLBinds.bindToSetType(batchIndex, paramValue);
                Packets.writeStringLenEnc(packet, value.getBytes(charset));
            }
            break;
            case NULL:
            case UNKNOWN:
            default: {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
            }

        }

    }


    /**
     * @see #writeBinary(ByteBuf, int, Value, int, Charset, ZoneOffset)
     */
    static MySQLType decideActualType(final MySQLType expectedType, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final MySQLType bindType;
        switch (expectedType) {
            case BIT: {
                // Server 8.0.27 and before ,can't bind BIT type.
                //@see writeBit method.
                bindType = MySQLType.BIGINT;
            }
            break;
            case YEAR: {
                //  Server 8.0.27 ,if bind YEAR type server response 'Malformed communication packet.'
                bindType = MySQLType.SMALLINT;
            }
            break;
            case DATETIME:
            case TIMESTAMP: {
                if (nonNull instanceof OffsetDateTime || nonNull instanceof ZonedDateTime) {
                    //As of MySQL 8.0.19 can append zone
                    bindType = MySQLType.VARCHAR;
                } else {
                    bindType = expectedType;
                }
            }
            break;
            default: {
                bindType = expectedType;
            }
        }
        return bindType;
    }

    private static void writeString(final ByteBuf packet, final int batchIndex, final Value paramValue,
                                    final Charset charset) {
        final Object nonNull = paramValue.getNonNull();
        if (nonNull instanceof byte[]) {
            final byte[] bytes = (byte[]) nonNull;
            if (StandardCharsets.UTF_8.equals(charset)) {
                Packets.writeStringLenEnc(packet, bytes);
            } else {
                final byte[] textBytes = new String(bytes, StandardCharsets.UTF_8).getBytes(charset);
                Packets.writeStringLenEnc(packet, textBytes);
            }
        } else {
            final String value;
            value = MySQLBinds.bindToString(batchIndex, paramValue);
            Packets.writeStringLenEnc(packet, value.getBytes(charset));
        }
    }


    /**
     * @see #decideActualType(MySQLType, Value)
     * @see #writeBinary(ByteBuf, int, Value, int, Charset, ZoneOffset)
     */
    private static void writeBit(final ByteBuf packet, final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNullValue();
        final long value;
        if (nonNull instanceof Long) {
            value = (Long) nonNull;
        } else if (nonNull instanceof Integer) {
            value = (Integer) nonNull & 0xFFFF_FFFFL;
        } else if (nonNull instanceof Short) {
            value = (Short) nonNull & 0xFFFFL;
        } else if (nonNull instanceof Byte) {
            value = (Byte) nonNull & 0xFFL;
        } else if (nonNull instanceof BitSet) {
            final BitSet v = (BitSet) nonNull;
            if (v.length() > 64) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = v.toLongArray()[0];
        } else if (nonNull instanceof String) {
            value = Long.parseUnsignedLong((String) nonNull, 2);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        // MySQL server 8.0.27 and before don't support send MYSQL_TYPE_BIT
        Packets.writeInt8(packet, value);
    }


    /**
     * <p>
     * write following type :
     *     <ul>
     *         <li>{@link LocalTime}</li>
     *         <li>{@link String}</li>
     *         <li>{@link OffsetTime}</li>
     *     </ul>
     *     to {@link MySQLType#TIME}
     * </p>
     *
     * @param precision  negative if dont' need to truncate micro seconds.
     * @param serverZone null if as of MySQL 8.0.19 ,now binary protocol parameter type is varchar
     *                   when {@link Value#getNonNullValue()} is {@link OffsetTime} type.
     * @see #writeBinary(ByteBuf, int, Value, int, Charset, ZoneOffset)
     */
    private static void writeTime(final ByteBuf packet, final int batchIndex, final Value paramValue,
                                  final int precision, final ZoneOffset serverZone) {
        final Object nonNull = paramValue.getNonNullValue();

        if (nonNull instanceof LocalTime || nonNull instanceof String) {
            final LocalTime value;
            if (nonNull instanceof LocalTime) {
                value = (LocalTime) nonNull;
            } else {
                value = LocalTime.parse((String) nonNull, MySQLTimes.TIME_FORMATTER_6);
            }
            writeLocalTime(packet, MySQLTimes.truncatedIfNeed(precision, value));
        } else if (nonNull instanceof Duration) {
            if (MySQLTimes.isOverflowDuration((Duration) nonNull)) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            writeDuration(packet, precision, (Duration) nonNull);
        } else if (nonNull instanceof OffsetTime) {
            final OffsetTime value;
            value = MySQLTimes.truncatedIfNeed(precision, (OffsetTime) nonNull);
            writeLocalTime(packet, value.withOffsetSameInstant(serverZone).toLocalTime());
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }


    }

    /**
     * <p>
     * write {@link Duration} with MySQL binary MYSQL_TYPE_TIME protocol.
     * </p>
     *
     * @see #writeTime(ByteBuf, int, Value, int, ZoneOffset)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time">MYSQL_TYPE_TIME</a>
     */
    private static void writeDuration(final ByteBuf packet, final int precision, final Duration value) {
        final Duration duration;
        final boolean negative = value.isNegative();
        if (negative) {
            duration = value.negated();
        } else {
            duration = value;
        }
        final int microSeconds;
        microSeconds = truncateMicroSecondsIfNeed(duration.getNano() / 1000, precision);

        packet.writeByte(microSeconds > 0 ? 12 : 8); //1. length
        packet.writeByte(negative ? 1 : 0); //2. is_negative

        long totalSeconds = duration.getSeconds();
        Packets.writeInt4(packet, (int) (totalSeconds / (3600 * 24))); //3. days
        totalSeconds %= (3600 * 24);

        packet.writeByte((int) (totalSeconds / 3600)); //4. hour
        totalSeconds %= 3600;

        packet.writeByte((int) (totalSeconds / 60)); //5. minute
        totalSeconds %= 60;

        packet.writeByte((int) totalSeconds); //6. second
        if (microSeconds > 0) {
            Packets.writeInt4(packet, microSeconds); // microsecond
        }


    }

    /**
     * <p>
     * write {@link LocalTime} with MySQL binary MYSQL_TYPE_TIME protocol.
     * </p>
     *
     * @see #writeTime(ByteBuf, int, Value, int, ZoneOffset)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time">MYSQL_TYPE_TIME</a>
     */
    private static void writeLocalTime(final ByteBuf packet, final LocalTime value) {
        final int microSeconds;
        microSeconds = value.get(ChronoField.MICRO_OF_SECOND);

        packet.writeByte(microSeconds > 0 ? 12 : 8); //1. length
        packet.writeByte(0); //2. is_negative
        packet.writeZero(4); //3. days

        packet.writeByte(value.getHour()); //4. hour
        packet.writeByte(value.getMinute()); //5. minute
        packet.writeByte(value.getSecond()); ///6. second
        if (microSeconds > 0) {
            Packets.writeInt4(packet, microSeconds); // microsecond
        }

    }


    /**
     * <p>
     * write following type :
     *     <ul>
     *         <li>{@link LocalDateTime}</li>
     *         <li>{@link String}</li>
     *         <li>{@link OffsetDateTime}</li>
     *         <li>{@link ZonedDateTime}</li>
     *     </ul>
     *     to {@link MySQLType#DATETIME} or {@link MySQLType#TIMESTAMP}
     * </p>
     *
     * @param precision  negative if dont' need to truncate micro seconds.
     * @param serverZone null if as of MySQL 8.0.19 ,now binary protocol parameter type is varchar
     *                   when {@link Value#getNonNullValue()} is following type:
     *                   <ul>
     *                      <li>{@link OffsetDateTime}</li>
     *                      <li>{@link ZonedDateTime}</li>
     *                   </ul>
     * @see #writeBinary(ByteBuf, int, Value, int, Charset, ZoneOffset)
     */
    private static void writeDatetime(final ByteBuf packet, final int batchIndex, final Value paramValue,
                                      final int precision, final Charset clientCharset,
                                      final @Nullable ZoneOffset serverZone) {
        final Object nonNull = paramValue.getNonNullValue();

        if (nonNull instanceof OffsetDateTime) {
            final OffsetDateTime value;
            value = JdbdTimes.truncatedIfNeed(precision, (OffsetDateTime) nonNull);
            if (serverZone == null) {
                final byte[] bytes;
                bytes = value.format(JdbdTimes.OFFSET_DATETIME_FORMATTER_6).getBytes(clientCharset);
                Packets.writeStringLenEnc(packet, bytes);
            } else {
                writeLocalDateTime(packet, value.withOffsetSameInstant(serverZone).toLocalDateTime());
            }
        } else if (nonNull instanceof ZonedDateTime) {
            final ZonedDateTime value;
            value = JdbdTimes.truncatedIfNeed(precision, (ZonedDateTime) nonNull);
            if (serverZone == null) {
                final byte[] bytes;
                bytes = value.format(JdbdTimes.OFFSET_DATETIME_FORMATTER_6).getBytes(clientCharset);
                Packets.writeStringLenEnc(packet, bytes);
            } else {
                writeLocalDateTime(packet, value.withZoneSameInstant(serverZone).toLocalDateTime());
            }
        } else {
            final LocalDateTime value;
            value = JdbdTimes.truncatedIfNeed(precision, MySQLBinds.bindToLocalDateTime(batchIndex, paramValue));
            writeLocalDateTime(packet, value);
        }

    }

    /**
     * <p>
     * write {@link LocalDateTime} with MySQL binary MYSQL_TYPE_DATETIME protocol.
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date">MYSQL_TYPE_DATETIME</a>
     */
    private static void writeLocalDateTime(final ByteBuf packet, final LocalDateTime value) {

        final int microSeconds;
        microSeconds = value.get(ChronoField.MICRO_OF_SECOND);

        packet.writeByte(microSeconds > 0 ? 11 : 7); // length ,always have micro second ,because for BindStatement
        Packets.writeInt2(packet, value.getYear()); // year
        packet.writeByte(value.getMonthValue()); // month
        packet.writeByte(value.getDayOfMonth()); // day

        packet.writeByte(value.getHour()); // hour
        packet.writeByte(value.getMinute()); // minute
        packet.writeByte(value.getSecond()); // second
        if (microSeconds > 0) {
            Packets.writeInt4(packet, microSeconds); // microsecond
        }

    }


    /**
     * @see #writeDuration(ByteBuf, int, Duration)
     */
    private static int truncateMicroSecondsIfNeed(final int microSeconds, final int precision) {
        final int newMicroSeconds;
        switch (precision) {
            case 0:
                newMicroSeconds = 0;
                break;
            case 1:
                newMicroSeconds = microSeconds - (microSeconds % 100000);
                break;
            case 2:
                newMicroSeconds = microSeconds - (microSeconds % 10000);
                break;
            case 3:
                newMicroSeconds = microSeconds - (microSeconds % 1000);
                break;
            case 4:
                newMicroSeconds = microSeconds - (microSeconds % 100);
                break;
            case 5:
                newMicroSeconds = microSeconds - (microSeconds % 10);
                break;
            case 6:
            default:
                newMicroSeconds = microSeconds;
        }
        return newMicroSeconds;
    }


}
