package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.*;
import java.util.function.Consumer;


/**
 * @see ComPreparedTask
 */
final class BinaryResultSetReader extends AbstractResultSetReader {

    static BinaryResultSetReader create(StmtTask task) {
        return new BinaryResultSetReader(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(BinaryResultSetReader.class);

    private static final byte BINARY_ROW_HEADER = 0x00;


    private BinaryResultSetReader(StmtTask task) {
        super(task);
    }

    @Override
    boolean readResultSetMeta(final ByteBuf cumulateBuffer, final Consumer<Object> statesConsumer) {
        final boolean metaEnd;
        if (MySQLRowMeta.canReadMeta(cumulateBuffer, false)) {
            doReadRowMeta(cumulateBuffer);
            metaEnd = true;
        } else {
            metaEnd = false;
        }
        return metaEnd;
    }


    @Override
    Logger getLogger() {
        return LOG;
    }


    @Override
    ResultRow readOneRow(final ByteBuf cumulateBuffer, final MySQLRowMeta rowMeta) {
        final MySQLColumnMeta[] columnMetas = rowMeta.columnMetaArray;
        if (cumulateBuffer.readByte() != BINARY_ROW_HEADER) {
            throw new IllegalArgumentException("cumulateBuffer isn't binary row");
        }
        final byte[] nullBitMap = new byte[(columnMetas.length + 9) / 8];
        cumulateBuffer.readBytes(nullBitMap); // null_bitmap

        final Object[] columnValues = new Object[columnMetas.length];
        for (int i = 0, byteIndex, bitIndex; i < columnMetas.length; i++) {
            MySQLColumnMeta columnMeta = columnMetas[i];
            byteIndex = (i + 2) & (~7);
            bitIndex = (i + 2) & 7;
            if ((nullBitMap[byteIndex] & (1 << bitIndex)) != 0) {
                continue;
            }
            columnValues[i] = readColumnValue(cumulateBuffer, columnMeta);
        }
        return MySQLResultRow.from(columnValues, rowMeta, this.adjutant);
    }

    /**
     * @return maybe null ,only when {@code DATETIME} is zero.
     */
    @SuppressWarnings("deprecation")
    @Nullable
    @Override
    Object readColumnValue(final ByteBuf cumulateBuffer, final MySQLColumnMeta meta) {
        final Charset columnCharset = this.adjutant.obtainColumnCharset(meta.columnCharset);
        final Object value;
        final byte[] bytes;
        switch (meta.sqlType) { // binary protocol must switch columnMeta.typeFlag
            case TINYINT: {
                value = cumulateBuffer.readByte();
            }
            break;
            case TINYINT_UNSIGNED: {
                value = (short) (cumulateBuffer.readByte() & 0xFF);
            }
            break;
            case SMALLINT: {
                value = (short) Packets.readInt2AsInt(cumulateBuffer);
            }
            break;
            case SMALLINT_UNSIGNED: {
                value = Packets.readInt2AsInt(cumulateBuffer);
            }
            break;
            case MEDIUMINT: {
                final int v = Packets.readInt3(cumulateBuffer);
                if ((v & 0x80_00_00) == 0) {//positive
                    value = v;
                } else {//negative
                    final int complement = (v & 0x7F_FF_FF);
                    if (complement == 0) {
                        value = (-(0x7F_FF_FF) - 1);
                    } else {
                        value = -((complement - 1) ^ 0x7F_FF_FF);
                    }
                }
            }
            break;
            case MEDIUMINT_UNSIGNED: {
                value = Packets.readInt3(cumulateBuffer);
            }
            break;
            case INT: {
                value = Packets.readInt4(cumulateBuffer);
            }
            break;
            case INT_UNSIGNED: {
                value = Packets.readInt4AsLong(cumulateBuffer);
            }
            break;
            case BIGINT: {
                value = Packets.readInt8(cumulateBuffer);
            }
            break;
            case BIGINT_UNSIGNED: {
                value = Packets.readInt8AsBigInteger(cumulateBuffer);
            }
            break;
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(meta);
                }
                value = new BigDecimal(new String(bytes, columnCharset));
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                value = Float.intBitsToFloat(Packets.readInt4(cumulateBuffer));
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                value = Double.longBitsToDouble(Packets.readInt8(cumulateBuffer));
            }
            break;
            case BOOLEAN: {
                value = cumulateBuffer.readByte() != 0;
            }
            break;
            case BIT: {
                value = readBitType(cumulateBuffer);
                if (value == null) {
                    throw createResponseNullBinaryError(meta);
                }
            }
            break;
            case TIME: {
                value = readBinaryTimeType(cumulateBuffer);
            }
            break;
            case DATE: {
                value = readBinaryDate(cumulateBuffer);
            }
            break;
            case YEAR: {
                value = Year.of(Packets.readInt2AsInt(cumulateBuffer));
            }
            break;
            case TIMESTAMP:
            case DATETIME: {
                value = readBinaryDateTime(cumulateBuffer);
            }
            break;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case ENUM:
            case MEDIUMTEXT: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(meta);
                }
                value = new String(bytes, columnCharset);
            }
            break;
            case LONGTEXT:
            case JSON: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(meta);
                }
                value = LongStrings.fromString(new String(bytes, columnCharset));
            }
            break;
            case SET: {
                value = readSetType(cumulateBuffer, columnCharset);
                if (value == null) {
                    throw createResponseNullBinaryError(meta);
                }
            }
            break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB: {
                value = Packets.readBytesLenEnc(cumulateBuffer);
            }
            break;
            case LONGBLOB: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(meta);
                }
                value = LongBinaries.fromArray(bytes);
            }
            break;
            case GEOMETRY: {
                final int length = Packets.readLenEncAsInt(cumulateBuffer);
                cumulateBuffer.skipBytes(4);// skip geometry prefix
                bytes = new byte[length - 4];
                cumulateBuffer.readBytes(bytes);
                value = LongBinaries.fromArray(bytes);
            }
            break;
            case UNKNOWN:
            case NULL:
            default: {
                value = readUnknown(cumulateBuffer, meta, columnCharset);
            }

        }
        return value;
    }


    @Override
    boolean isBinaryReader() {
        return true;
    }


    /**
     * @return -1 : more cumulate.
     */
    long obtainColumnBytes(MySQLColumnMeta columnMeta, final ByteBuf payload) {
        final long columnBytes;
        switch (columnMeta.typeFlag) {
            case Constants.TYPE_STRING:
            case Constants.TYPE_VARCHAR:
            case Constants.TYPE_VAR_STRING:
            case Constants.TYPE_TINY_BLOB:
            case Constants.TYPE_BLOB:
            case Constants.TYPE_MEDIUM_BLOB:
            case Constants.TYPE_LONG_BLOB:
            case Constants.TYPE_GEOMETRY:
            case Constants.TYPE_NEWDECIMAL:
            case Constants.TYPE_DECIMAL:
            case Constants.TYPE_BIT:
            case Constants.TYPE_ENUM:
            case Constants.TYPE_SET:
            case Constants.TYPE_JSON: {
                columnBytes = Packets.getLenEncTotalByteLength(payload);
            }
            break;
            case Constants.TYPE_DOUBLE:
            case Constants.TYPE_LONGLONG: {
                columnBytes = 8L;
            }
            break;
            case Constants.TYPE_FLOAT:
            case Constants.TYPE_INT24:
            case Constants.TYPE_LONG: {
                columnBytes = 4L;
            }
            break;
            case Constants.TYPE_YEAR:
            case Constants.TYPE_SHORT: {
                columnBytes = 2L;
            }
            break;
            case Constants.TYPE_BOOL:
            case Constants.TYPE_TINY: {
                columnBytes = 1L;
            }
            break;
            case Constants.TYPE_TIMESTAMP:
            case Constants.TYPE_DATETIME:
            case Constants.TYPE_TIME:
            case Constants.TYPE_DATE: {
                columnBytes = 1L + payload.getByte(payload.readerIndex());
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("Server send unknown type[%s]"
                        , columnMeta.typeFlag);

        }
        return columnBytes;
    }

    /*################################## blow private method ##################################*/


    /**
     * @return {@link LocalTime} or {@link Duration}
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time">ProtocolBinary::MYSQL_TYPE_TIME</a>
     */
    private Object readBinaryTimeType(ByteBuf byteBuf) {
        final int length = byteBuf.readByte();
        if (length == 0) {
            return LocalTime.MIDNIGHT;
        }
        final Object value;
        final boolean negative = byteBuf.readByte() == 1;
        final int days, hours, minutes, seconds;
        days = Packets.readInt4(byteBuf);
        hours = byteBuf.readByte();
        minutes = byteBuf.readByte();
        seconds = byteBuf.readByte();

        if (negative || days != 0 || hours > 23) {
            int totalSeconds = (days * 24 * 3600) + (hours * 3600) + (minutes * 60) + seconds;
            if (negative) {
                totalSeconds = -totalSeconds;
            }
            if (length == 8) {
                value = Duration.ofSeconds(totalSeconds);
            } else {
                value = Duration.ofSeconds(totalSeconds, Packets.readInt4(byteBuf) * 1000L);
            }
        } else if (length == 8) {
            value = LocalTime.of(hours, minutes, seconds);
        } else {
            value = LocalTime.of(hours, minutes, seconds, Packets.readInt4(byteBuf) * 1000);
        }
        return value;
    }


    /**
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date">ProtocolBinary::MYSQL_TYPE_DATETIME</a>
     */
    @Nullable
    private LocalDateTime readBinaryDateTime(ByteBuf payload) {
        LocalDateTime dateTime;
        final byte length = payload.readByte();
        switch (length) {
            case 7: {
                dateTime = LocalDateTime.of(
                        Packets.readInt2AsInt(payload) // year
                        , payload.readByte()           // month
                        , payload.readByte()           // day

                        , payload.readByte()           // hour
                        , payload.readByte()           // minute
                        , payload.readByte()           // second
                );
            }
            break;
            case 11: {
                dateTime = LocalDateTime.of(
                        Packets.readInt2AsInt(payload) // year
                        , payload.readByte()           // month
                        , payload.readByte()           // day

                        , payload.readByte()           // hour
                        , payload.readByte()           // minute
                        , payload.readByte()           // second

                        , Packets.readInt4(payload) * 1000 // micro second
                );
            }
            break;
            case 4: {
                LocalDate date = LocalDate.of(Packets.readInt2AsInt(payload), payload.readByte(), payload.readByte());
                dateTime = LocalDateTime.of(date, LocalTime.MIN);
            }
            break;
            case 0: {
                dateTime = null;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException(
                        String.format("Server send binary MYSQL_TYPE_DATETIME length[%s] error.", length));
        }

        if (dateTime == null) {
            LocalDate date = handleZeroDateBehavior("DATETIME");
            if (date != null) {
                dateTime = LocalDateTime.of(date, LocalTime.MIDNIGHT);
            }
        }
        return dateTime;
    }

    /**
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_date">ProtocolBinary::MYSQL_TYPE_DATE</a>
     */
    @Nullable
    private LocalDate readBinaryDate(ByteBuf payload) {
        final int length = payload.readByte();
        LocalDate date;
        switch (length) {
            case 4:
                date = LocalDate.of(Packets.readInt2AsInt(payload), payload.readByte(), payload.readByte());
                break;
            case 0:
                date = null;
                break;
            default:
                throw MySQLExceptions.createFatalIoException(
                        String.format("Server send binary MYSQL_TYPE_DATE length[%s] error.", length));
        }
        if (date == null) {
            // maybe null
            date = handleZeroDateBehavior("DATE");
        }
        return date;
    }

    /*################################## blow static method ##################################*/

    /*################################## blow private static method ##################################*/

    private static MySQLJdbdException createResponseNullBinaryError(MySQLColumnMeta meta) {
        String m = String.format("MySQL server response null value in binary protocol,meta:%s", meta);
        return new MySQLJdbdException(m);
    }


}
