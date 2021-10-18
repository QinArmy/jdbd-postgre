package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

abstract class BinaryWriter {

    private BinaryWriter() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    static void writeNonNullBinary(ByteBuf packet, final int batchIndex, final MySQLType type
            , ParamValue paramValue, final Charset charset) throws SQLException {

        switch (type) {
            case BOOLEAN:
            case TINYINT: {
                packet.writeByte(MySQLBinds.bindNonNullToByte(batchIndex, type, paramValue));
            }
            break;
            case TINYINT_UNSIGNED: {
                final short value;
                value = MySQLBinds.bindNonNullToShort(batchIndex, type, paramValue);
                if ((value & (~0xFF)) != 0) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, type, paramValue);
                }
                packet.writeByte(value);
            }
            break;
            case SMALLINT: {
                final short value;
                value = MySQLBinds.bindNonNullToShort(batchIndex, type, paramValue);
                Packets.writeInt2(packet, value);
            }
            break;
            case SMALLINT_UNSIGNED: {
                final int value;
                value = MySQLBinds.bindNonNullToInt(batchIndex, type, paramValue);
                if ((value & (~0xFFFF)) != 0) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, type, paramValue);
                }
                Packets.writeInt2(packet, value);
            }
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT: {
                final int value;
                value = MySQLBinds.bindNonNullToInt(batchIndex, type, paramValue);
                Packets.writeInt4(packet, value);
            }
            break;
            case INT_UNSIGNED: {
                final long value;
                value = MySQLBinds.bindNonNullToLong(batchIndex, type, paramValue);
                if ((value & (~0xFFFF_FFFFL)) != 0) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, type, paramValue);
                }
                Packets.writeInt4(packet, (int) value);
            }
            break;
            case BIGINT: {
                final long value;
                value = MySQLBinds.bindNonNullToLong(batchIndex, type, paramValue);
                Packets.writeInt8(packet, value);
            }
            break;
            case BIGINT_UNSIGNED: {
                final BigInteger value;
                value = MySQLBinds.bindToBigInteger(batchIndex, type, paramValue);
                final byte[] bytes = value.toByteArray();
                if (value.compareTo(BigInteger.ZERO) < 0 || bytes.length > 9 || bytes[0] != 0) {
                    throw JdbdExceptions.outOfTypeRange(batchIndex, type, paramValue);
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
                value = MySQLBinds.bindNonNullToYear(batchIndex, type, paramValue);
                Packets.writeInt2(packet, value);
            }
            break;
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                final BigDecimal value;
                value = MySQLBinds.bindNonNullToDecimal(batchIndex, type, paramValue);
                Packets.writeStringLenEnc(packet, value.toPlainString().getBytes(charset));
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                final float value;
                value = MySQLBinds.bindNonNullToFloat(batchIndex, type, paramValue);
                Packets.writeInt4(packet, Float.floatToIntBits(value));
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                final double value;
                value = MySQLBinds.bindNonNullToDouble(batchIndex, type, paramValue);
                Packets.writeInt8(packet, Double.doubleToLongBits(value));
            }
            break;
            case SET: {
                final String value;
                value = MySQLBinds.bindNonNullToSetType(batchIndex, type, paramValue);
                Packets.writeStringLenEnc(packet, value.getBytes(charset));
            }
            break;
            case ENUM:
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
            case JSON: {
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
                    value = MySQLBinds.bindNonNullToString(batchIndex, type, paramValue);
                    Packets.writeStringLenEnc(packet, value.getBytes(charset));
                }
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
                    throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, type, paramValue);
                }
            }
            break;
            case DATE:
            case BIT: {

            }
            break;
            default: {
                throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, type, paramValue);
            }

        }

    }


}
