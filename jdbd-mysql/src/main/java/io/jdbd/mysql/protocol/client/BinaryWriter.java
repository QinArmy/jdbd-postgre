package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.vendor.stmt.Value;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import java.time.temporal.ChronoField;
import java.util.BitSet;
import java.util.Set;

abstract class BinaryWriter {

    private BinaryWriter() {
        throw new UnsupportedOperationException();
    }


    /**
     * @param expectedType from COM_PREPARE_STMT parameter metadata or query attribute bind method.
     * @param precision    0 or {@link MySQLType#TIME} and {@link MySQLType#DATETIME} precision
     */
    @SuppressWarnings("deprecation")
    static void writeNonNullBinary(ByteBuf packet, final int batchIndex, final MySQLType expectedType
            , Value paramValue, final int precision, final Charset charset) throws SQLException {

        switch (expectedType) {
            case BOOLEAN:
            case TINYINT: {
                packet.writeByte(MySQLBinds.bindToByte(batchIndex, expectedType, paramValue));
            }
            break;
            case TINYINT_UNSIGNED: {
                final short value;
                value = MySQLBinds.bindToShort(batchIndex, expectedType, paramValue);
                if ((value & (~0xFF)) != 0) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, expectedType, paramValue);
                }
                packet.writeByte(value);
            }
            break;
            case SMALLINT: {
                final int value;
                value = MySQLBinds.bindToShort(batchIndex, expectedType, paramValue);
                Packets.writeInt2(packet, value);
            }
            break;
            case SMALLINT_UNSIGNED: {
                final int value;
                value = MySQLBinds.bindToInt(batchIndex, expectedType, paramValue);
                if ((value & (~0xFFFF)) != 0) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, expectedType, paramValue);
                }
                Packets.writeInt2(packet, value);
            }
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT: {
                final int value;
                value = MySQLBinds.bindToInt(batchIndex, expectedType, paramValue);
                Packets.writeInt4(packet, value);
            }
            break;
            case INT_UNSIGNED: {
                final long value;
                value = MySQLBinds.bindToLong(batchIndex, expectedType, paramValue);
                if ((value & (~0xFFFF_FFFFL)) != 0) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, expectedType, paramValue);
                }
                Packets.writeInt4(packet, (int) value);
            }
            break;
            case BIGINT: {
                final long value;
                value = MySQLBinds.bindToLong(batchIndex, expectedType, paramValue);
                Packets.writeInt8(packet, value);
            }
            break;
            case BIGINT_UNSIGNED: {
                final BigInteger value;
                value = MySQLBinds.bindToBigInteger(batchIndex, expectedType, paramValue);
                final byte[] bytes = value.toByteArray();
                if (value.compareTo(BigInteger.ZERO) < 0 || bytes.length > 9 || (bytes.length == 9 && bytes[0] != 0)) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, expectedType, paramValue);
                }
                final byte[] int8Bytes = new byte[8];
                final int end = Math.min(int8Bytes.length, bytes.length);
                for (int i = 0, j = bytes.length - 1; i < end; i++, j--) {
                    int8Bytes[i] = bytes[j];
                }
                packet.writeBytes(int8Bytes);
            }
            break;
            case YEAR: {
                final int value;
                value = MySQLBinds.bindNonNullToYear(batchIndex, expectedType, paramValue);
                Packets.writeInt2(packet, value);
            }
            break;
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                final BigDecimal value;
                value = MySQLBinds.bindNonNullToDecimal(batchIndex, expectedType, paramValue);
                Packets.writeStringLenEnc(packet, value.toPlainString().getBytes(charset));
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                final float value;
                value = MySQLBinds.bindNonNullToFloat(batchIndex, expectedType, paramValue);
                Packets.writeInt4(packet, Float.floatToIntBits(value));
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                final double value;
                value = MySQLBinds.bindNonNullToDouble(batchIndex, expectedType, paramValue);
                Packets.writeInt8(packet, Double.doubleToLongBits(value));
            }
            break;
            case SET: {
                final String value;
                value = MySQLBinds.bindNonNullToSetType(batchIndex, expectedType, paramValue);
                Packets.writeStringLenEnc(packet, value.getBytes(charset));
            }
            break;
            case VARCHAR: {
                if (paramValue.getNonNull() instanceof Set) {
                    // Server response parameter metadata no MYSQL_TYPE_SET ,it's MYSQL_TYPE_VARCHAR
                    final String value;
                    value = MySQLBinds.bindNonNullToSetType(batchIndex, expectedType, paramValue);
                    Packets.writeStringLenEnc(packet, value.getBytes(charset));
                } else {
                    writeString(packet, batchIndex, expectedType, paramValue, charset);
                }
            }
            break;
            case ENUM:
            case CHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
            case JSON: {
                writeString(packet, batchIndex, expectedType, paramValue, charset);
            }
            break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
            case GEOMETRY: {
                final Object nonNull = paramValue.getNonNull();
                if (nonNull instanceof byte[]) {
                    Packets.writeStringLenEnc(packet, (byte[]) nonNull);
                } else if (nonNull instanceof String) {
                    Packets.writeStringLenEnc(packet, ((String) nonNull).getBytes(charset));
                } else {
                    throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, expectedType, paramValue);
                }
            }
            break;
            case TIMESTAMP:
            case DATETIME: {
                writeDatetime(packet, batchIndex, expectedType, paramValue, precision, charset);
            }
            break;
            case TIME: {
                writeTime(packet, batchIndex, expectedType, paramValue, precision);
            }
            break;
            case DATE: {
                final LocalDate value;
                value = MySQLBinds.bindNonNullToLocalDate(batchIndex, expectedType, paramValue);

                packet.writeByte(4); // length
                Packets.writeInt2(packet, value.getYear());// year
                packet.writeByte(value.getMonthValue());// month
                packet.writeByte(value.getDayOfMonth());// day
            }
            break;
            case BIT: {
                writeBit(packet, batchIndex, expectedType, paramValue);
            }
            break;
            case NULL:
            case UNKNOWN:
            default: {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, expectedType, paramValue);
            }

        }

    }

    private static void writeString(ByteBuf packet, final int batchIndex, final MySQLType expectedType
            , Value paramValue, Charset charset)
            throws SQLException {
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
            value = MySQLBinds.bindNonNullToString(batchIndex, expectedType, paramValue);
            Packets.writeStringLenEnc(packet, value.getBytes(charset));
        }
    }


    /**
     * @param expectedType from COM_PREPARE_STMT parameter metadata or query attribute bind method.
     * @see #decideActualType(MySQLType, Value)
     */
    private static void writeBit(ByteBuf packet, final int batchIndex, final MySQLType expectedType, Value paramValue)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
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
                throw JdbdExceptions.outOfTypeRange(batchIndex, expectedType, paramValue);
            }
            value = v.toLongArray()[0];
        } else if (nonNull instanceof String) {
            value = Long.parseUnsignedLong((String) nonNull, 2);
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, expectedType, paramValue);
        }
        // MySQL server 8.0.27 and before don't support send MYSQL_TYPE_BIT
        Packets.writeInt8(packet, value);
    }


    private static void writeTime(ByteBuf packet, final int batchIndex, final MySQLType type, Value paramValue
            , final int precision)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();
        if (nonNull instanceof Duration) {
            Duration duration = (Duration) nonNull;
            final boolean negative = duration.isNegative();
            if (negative) {
                duration = duration.negated();
            }

            if (!MySQLTimes.canConvertToTimeType(duration)) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, type, paramValue);
            }
            final int microSeconds = truncateMicroSeconds(duration.getNano() / 1000, precision);
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
        } else {
            final LocalTime value;
            value = MySQLBinds.bindNonNullToLocalTime(batchIndex, type, paramValue);
            final int microSeconds = truncateMicroSeconds(value.get(ChronoField.MICRO_OF_SECOND), precision);
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


    }

    private static void writeDatetime(ByteBuf packet, final int batchIndex, MySQLType type, Value paramValue
            , final int precision, Charset clientCharset)
            throws SQLException {
        final Object nonNull = paramValue.getNonNull();

        if (nonNull instanceof OffsetDateTime) {
            final byte[] bytes;
            bytes = ((OffsetDateTime) nonNull).format(MySQLTimes.getDateTimeFormatter(precision))
                    .getBytes(clientCharset);
            Packets.writeStringLenEnc(packet, bytes);
        } else if (nonNull instanceof ZonedDateTime) {
            final byte[] bytes;
            bytes = ((ZonedDateTime) nonNull).toOffsetDateTime().format(MySQLTimes.getDateTimeFormatter(precision))
                    .getBytes(clientCharset);
            Packets.writeStringLenEnc(packet, bytes);
        } else {
            final LocalDateTime value;
            value = MySQLBinds.bindNonNullToLocalDateTime(batchIndex, type, paramValue);
            final int microSeconds = truncateMicroSeconds(value.get(ChronoField.MICRO_OF_SECOND), precision);

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


    }


    /**
     * @see #writeTime(ByteBuf, int, MySQLType, Value, int)
     * @see #writeDatetime(ByteBuf, int, MySQLType, Value, int, Charset)
     */
    private static int truncateMicroSeconds(final int microSeconds, final int precision) {
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
                newMicroSeconds = microSeconds;
                break;
            default:
                throw new IllegalArgumentException(String.format("precision[%s] not in [0,6]", precision));
        }
        return newMicroSeconds;
    }


    static MySQLType decideActualType(final MySQLType expectedType, final Value paramValue) {
        final Object nonNull = paramValue.getNonNull();
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
}
