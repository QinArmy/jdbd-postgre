package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLNumbers;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.result.ErrorResultRow;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.*;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;


/**
 * @see ComPreparedTask
 */
final class BinaryResultSetReader extends AbstractResultSetReader {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryResultSetReader.class);


    BinaryResultSetReader(ResultSetReaderBuilder builder) {
        super(builder);
    }


    @Override
    boolean readResultSetMeta(ByteBuf cumulateBuffer, Consumer<Object> statesConsumer) {
        return doReadRowMeta(cumulateBuffer);
    }


    @Override
    Logger getLogger() {
        return LOG;
    }


    /**
     * @see #readResultRows(ByteBuf, Consumer)
     */
    @Override
    final ResultRow readOneRow(final ByteBuf cumulateBuffer, final MySQLRowMeta rowMeta) {
        final MySQLRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
        final MySQLColumnMeta[] columnMetas = rowMeta.columnMetaArray;
        if (payload.readByte() != 0) {
            throw MySQLExceptions.createFatalIoException(
                    "Header[%s] of Binary Protocol ResultSet Row is error."
                    , payload.getByte(payload.readerIndex() - 1));
        }
        final byte[] nullBitMap = new byte[(columnMetas.length + 9) / 8];
        payload.readBytes(nullBitMap); // null_bitmap

        final Object[] columnValues = new Object[columnMetas.length];
        ResultRow resultRow;
        try {
            for (int i = 0, byteIndex, bitIndex; i < columnMetas.length; i++) {
                MySQLColumnMeta columnMeta = columnMetas[i];
                byteIndex = (i + 2) & (~7);
                bitIndex = (i + 2) & 7;
                if ((nullBitMap[byteIndex] & (1 << bitIndex)) != 0) {
                    continue;
                }
                columnValues[i] = readColumnValue(payload, columnMeta);
            }
            resultRow = MySQLResultRow.from(columnValues, rowMeta, this.adjutant);
        } catch (Throwable e) {
            emitError(MySQLExceptions.wrap(e));
            resultRow = ErrorResultRow.INSTANCE;
        }
        return resultRow;
    }

    /**
     * @return maybe null ,only when {@code DATETIME} is zero.
     */
    @Nullable
    @Override
    Object readColumnValue(final ByteBuf payload, final MySQLColumnMeta columnMeta) {
        final Charset columnCharset = this.adjutant.obtainColumnCharset(columnMeta.columnCharset);
        final Object columnValue;
        byte[] bytes;
        switch (columnMeta.mysqlType) {
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                bytes = Packets.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = new BigDecimal(new String(bytes, columnCharset));
                }
            }
            break;
            case INT:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                columnValue = Packets.readInt4(payload);
                break;
            case INT_UNSIGNED:
                columnValue = Packets.readInt4AsLong(payload);
                break;
            case BIGINT:
                columnValue = Packets.readInt8(payload);
                break;
            case BIGINT_UNSIGNED: {
                bytes = new byte[9];
                bytes[0] = 0;
                payload.readBytes(bytes, 1, 8);
                byte temp;
                for (int left = 1, right = 8; left <= 4; left++, right--) {
                    temp = bytes[left];
                    bytes[left] = bytes[right];
                    bytes[right] = temp;
                }
                columnValue = new BigInteger(bytes);
            }
            break;
            case DATE:
                columnValue = readBinaryDate(payload);
                break;
            case TIMESTAMP:
            case DATETIME:
                columnValue = readBinaryDateTime(payload);
                break;
            case TIME:
                columnValue = readBinaryTimeType(payload);
                break;
            case CHAR:
            case VARCHAR:
            case ENUM:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
                columnValue = Packets.readStringLenEnc(payload, columnCharset);
                break;
            case LONGTEXT:
            case JSON: {
                bytes = Packets.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = LongStrings.fromString(new String(bytes, columnCharset));
                }
            }
            break;
            case BIT: {
                if (columnMeta.isTiny1AsBit()) {
                    columnValue = (long) payload.readByte();
                } else {
                    bytes = Packets.readBytesLenEnc(payload);
                    if (bytes == null) {
                        columnValue = null;
                    } else {
                        columnValue = MySQLNumbers.readLongFromBigEndian(bytes, 0, bytes.length);
                    }
                }
            }
            break;
            case SET: {
                bytes = Packets.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = MySQLConvertUtils.convertToSetType(new String(bytes, columnCharset));
                }
            }
            break;
            case SMALLINT:
                columnValue = (short) Packets.readInt2AsInt(payload);
                break;
            case SMALLINT_UNSIGNED:
                columnValue = Packets.readInt2AsInt(payload);
                break;
            case YEAR:
                columnValue = Year.of(Packets.readInt2AsInt(payload));
                break;
            case BOOLEAN:
                columnValue = MySQLConvertUtils.tryConvertToBoolean(payload.readByte());
                break;
            case TINYINT:
                columnValue = payload.readByte();
                break;
            case TINYINT_UNSIGNED:
                columnValue = payload.readUnsignedByte();
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED://UNSIGNED, if specified, disallows negative values. As of MySQL 8.0.17, the UNSIGNED attribute is deprecated
                columnValue = Double.longBitsToDouble(Packets.readInt8(payload));
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:// UNSIGNED, if specified, disallows negative values. As of MySQL 8.0.17, the UNSIGNED attribute is deprecated for columns
                columnValue = Float.intBitsToFloat(Packets.readInt4(payload));
                break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
                columnValue = Packets.readBytesLenEnc(payload);
                break;
            case LONGBLOB: {
                bytes = Packets.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = LongBinaries.fromArray(bytes);
                }
            }
            break;
            case GEOMETRY: {
                bytes = Packets.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = LongBinaries.fromArray(Arrays.copyOfRange(bytes, 4, bytes.length));
                }
            }
            break;
            case NULL:
            case UNKNOWN:
                throw MySQLExceptions.createFatalIoException(
                        String.format("Server return error type[%s] alias[%s]."
                                , columnMeta.mysqlType, columnMeta.columnLabel));
            default:
                throw MySQLExceptions.createUnexpectedEnumException(columnMeta.mysqlType);

        }
        return columnValue;
    }

    @Override
    boolean isBinaryReader() {
        return true;
    }

    @Override
    int skipNullColumn(BigRowData bigRowData, final ByteBuf payload, final int columnIndex) {
        final MySQLColumnMeta[] columnMetaArray = this.rowMeta.columnMetaArray;
        int i = columnIndex;
        final byte[] nullBitMap = bigRowData.bigRowNullBitMap;
        for (int byteIndex, bitIndex; i < columnMetaArray.length; i++) {
            byteIndex = (i + 2) / 8;
            bitIndex = (i + 2) % 8;
            if ((nullBitMap[byteIndex] & (1 << bitIndex)) == 0) {
                break;
            }
        }
        return i;
    }


    /**
     * @return negative : more cumulate.
     */
    long obtainColumnBytes(MySQLColumnMeta columnMeta, final ByteBuf bigPayloadBuffer) {
        final long columnBytes;
        switch (columnMeta.typeFlag) {
            case ProtocolConstants.TYPE_STRING:
            case ProtocolConstants.TYPE_VARCHAR:
            case ProtocolConstants.TYPE_VAR_STRING:
            case ProtocolConstants.TYPE_TINY_BLOB:
            case ProtocolConstants.TYPE_BLOB:
            case ProtocolConstants.TYPE_MEDIUM_BLOB:
            case ProtocolConstants.TYPE_LONG_BLOB:
            case ProtocolConstants.TYPE_GEOMETRY:
            case ProtocolConstants.TYPE_NEWDECIMAL:
            case ProtocolConstants.TYPE_DECIMAL:
            case ProtocolConstants.TYPE_BIT:
            case ProtocolConstants.TYPE_ENUM:
            case ProtocolConstants.TYPE_SET:
            case ProtocolConstants.TYPE_JSON: {
                columnBytes = Packets.getLenEncTotalByteLength(bigPayloadBuffer);
            }
            break;
            case ProtocolConstants.TYPE_DOUBLE:
            case ProtocolConstants.TYPE_LONGLONG: {
                columnBytes = 8L;
            }
            break;
            case ProtocolConstants.TYPE_FLOAT:
            case ProtocolConstants.TYPE_INT24:
            case ProtocolConstants.TYPE_LONG: {
                columnBytes = 4L;
            }
            break;
            case ProtocolConstants.TYPE_YEAR:
            case ProtocolConstants.TYPE_SHORT: {
                columnBytes = 2L;
            }
            break;
            case ProtocolConstants.TYPE_BOOL:
            case ProtocolConstants.TYPE_TINY: {
                columnBytes = 1L;
            }
            break;
            case ProtocolConstants.TYPE_TIMESTAMP:
            case ProtocolConstants.TYPE_DATETIME:
            case ProtocolConstants.TYPE_TIME:
            case ProtocolConstants.TYPE_DATE: {
                columnBytes = 1L + bigPayloadBuffer.getByte(bigPayloadBuffer.readerIndex());
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
        Object value;
        if (length == 0) {
            value = LocalTime.MIN;
        } else {
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
        }
        if (value instanceof LocalTime) {
            value = OffsetTime.of((LocalTime) value, this.adjutant.obtainZoneOffsetDatabase())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                    .toLocalTime();
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
                dateTime = LocalDateTime.of(date, LocalTime.MIN);
            }
        } else {
            dateTime = OffsetDateTime.of(dateTime, this.adjutant.obtainZoneOffsetDatabase())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                    .toLocalDateTime();
        }
        return dateTime;
    }


    /*################################## blow static method ##################################*/

    /*################################## blow private static method ##################################*/

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


}
