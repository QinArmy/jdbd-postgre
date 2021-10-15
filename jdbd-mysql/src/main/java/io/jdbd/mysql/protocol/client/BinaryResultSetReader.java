package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLNumbers;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.*;
import java.util.Arrays;
import java.util.function.Consumer;


/**
 * @see ComPreparedStmtTask
 */
final class BinaryResultSetReader extends AbstractResultSetReader {

    static BinaryResultSetReader create(StmtTask stmtTask, ResultSink sink) {
        return new BinaryResultSetReader(stmtTask, sink);
    }

    private static final Logger LOG = LoggerFactory.getLogger(BinaryResultSetReader.class);

    private static final byte BINARY_ROW_HEADER = 0x00;


    private BinaryResultSetReader(StmtTask stmtTask, ResultSink sink) {
        super(stmtTask, sink);
    }


    @Override
    public final boolean isResettable() {
        return true;
    }

    @Override
    final boolean readResultSetMeta(final ByteBuf cumulateBuffer, final Consumer<Object> statesConsumer) {
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
    final Logger getLogger() {
        return LOG;
    }


    /**
     * @see #readResultRows(ByteBuf, Consumer)
     */
    @Override
    final ResultRow readOneRow(final ByteBuf cumulateBuffer, final MySQLRowMeta rowMeta) {
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
    @Nullable
    @Override
    Object readColumnValue(final ByteBuf cumulateBuffer, final MySQLColumnMeta columnMeta) {
        final Charset columnCharset = this.adjutant.obtainColumnCharset(columnMeta.columnCharset);
        final Object value;
        final byte[] bytes;
        switch (columnMeta.typeFlag) { // binary protocol must switch columnMeta.typeFlag
            case Constants.TYPE_TINY: {
                value = columnMeta.isUnsigned() ? cumulateBuffer.readUnsignedByte() : cumulateBuffer.readByte();
            }
            break;
            case Constants.TYPE_SHORT: {
                if (columnMeta.isUnsigned()) {
                    value = Packets.readInt2AsInt(cumulateBuffer);
                } else {
                    value = (short) Packets.readInt2AsInt(cumulateBuffer);
                }
            }
            break;
            case Constants.TYPE_INT24: {
                value = Packets.readInt3(cumulateBuffer);
            }
            break;
            case Constants.TYPE_LONG: {
                if (columnMeta.isUnsigned()) {
                    value = Packets.readInt4AsLong(cumulateBuffer);
                } else {
                    value = Packets.readInt4(cumulateBuffer);
                }
            }
            break;
            case Constants.TYPE_LONGLONG: {
                if (columnMeta.isUnsigned()) {
                    value = Packets.readInt8AsBigInteger(cumulateBuffer);
                } else {
                    value = Packets.readInt8(cumulateBuffer);
                }
            }
            break;
            case Constants.TYPE_DECIMAL:
            case Constants.TYPE_NEWDECIMAL: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                value = new BigDecimal(new String(bytes, columnCharset));
            }
            break;
            case Constants.TYPE_FLOAT: {
                value = Float.intBitsToFloat(Packets.readInt4(cumulateBuffer));
            }
            break;
            case Constants.TYPE_DOUBLE: {
                value = Double.longBitsToDouble(Packets.readInt4(cumulateBuffer));
            }
            break;
            case Constants.TYPE_BOOL: {
                value = cumulateBuffer.readByte() != 0;
            }
            break;
            case Constants.TYPE_NULL: // null as bytes
            case Constants.TYPE_STRING:
            case Constants.TYPE_VAR_STRING:
            case Constants.TYPE_VARCHAR:
            case Constants.TYPE_ENUM:
            case Constants.TYPE_TINY_BLOB:
            case Constants.TYPE_BLOB:
            case Constants.TYPE_MEDIUM_BLOB: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                if (columnMeta.sqlType == MySQLType.ENUM || columnMeta.sqlType.isStringType()) {
                    value = new String(bytes, columnCharset);
                } else {
                    value = bytes;
                }
            }
            break;
            case Constants.TYPE_BIT: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                value = MySQLNumbers.readLongFromBigEndian(bytes, 0, bytes.length);
            }
            break;

            case Constants.TYPE_DATE: {
                value = readBinaryDate(cumulateBuffer);
            }
            break;
            case Constants.TYPE_TIMESTAMP:
            case Constants.TYPE_DATETIME: {
                value = readBinaryDateTime(cumulateBuffer);
            }
            break;
            case Constants.TYPE_TIME: {
                value = readBinaryTimeType(cumulateBuffer);
            }
            break;
            case Constants.TYPE_LONG_BLOB: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                if (columnMeta.sqlType.isStringType()) {
                    value = LongStrings.fromString(new String(bytes, columnCharset));
                } else {
                    value = LongBinaries.fromArray(bytes);
                }
            }
            break;
            case Constants.TYPE_JSON: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                value = LongStrings.fromString(new String(bytes, columnCharset));
            }
            break;
            case Constants.TYPE_SET: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                value = MySQLConvertUtils.convertToSetType(new String(bytes, columnCharset));
            }
            break;
            case Constants.TYPE_YEAR: {
                value = Year.of(Packets.readInt2AsInt(cumulateBuffer));
            }
            break;
            case Constants.TYPE_GEOMETRY: {
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                value = LongBinaries.fromArray(Arrays.copyOfRange(bytes, 4, bytes.length));// skip geometry prefix
            }
            break;
            default: {
                // unknown type
                bytes = Packets.readBytesLenEnc(cumulateBuffer);
                if (bytes == null) {
                    throw createResponseNullBinaryError(columnMeta);
                }
                value = LongBinaries.fromArray(bytes);
            }

        }
        return value;
    }


    @Override
    final boolean isBinaryReader() {
        return true;
    }


    /**
     * @return -1 : more cumulate.
     */
    final long obtainColumnBytes(MySQLColumnMeta columnMeta, final ByteBuf payload) {
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
