package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLNumberUtils;
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
        if (builder.resettable) {
            throw new IllegalArgumentException(String.format("%s can't reset.", this.getClass().getName()));
        }
    }


    @Override
    boolean readResultSetMeta(ByteBuf cumulateBuffer, Consumer<Object> statesConsumer) {
        return doReadRowMeta(cumulateBuffer);
    }


    @Override
    Logger obtainLogger() {
        return LOG;
    }

    @Override
    final boolean isResettable() {
        return true;
    }

    /**
     * @see #readResultRows(ByteBuf, Consumer)
     */
    @Override
    ResultRow readOneRow(final ByteBuf payload) {
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

        for (int i = 0, byteIndex, bitIndex; i < columnMetas.length; i++) {
            MySQLColumnMeta columnMeta = columnMetas[i];
            byteIndex = (i + 2) & (~7);
            bitIndex = (i + 2) & 7;
            if ((nullBitMap[byteIndex] & (1 << bitIndex)) != 0) {
                continue;
            }
            columnValues[i] = readColumnValue(payload, columnMeta);
        }
        return MySQLResultRow.from(columnValues, rowMeta, this.adjutant);
    }

    /**
     * @return maybe null ,only when {@code DATETIME} is zero.
     */
    @Nullable
    @Override
    Object internalReadColumnValue(final ByteBuf payload, final MySQLColumnMeta columnMeta) {
        final Charset columnCharset = this.adjutant.obtainColumnCharset(columnMeta.columnCharset);
        final Object columnValue;
        byte[] bytes;
        switch (columnMeta.mysqlType) {
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                bytes = PacketUtils.readBytesLenEnc(payload);
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
                columnValue = PacketUtils.readInt4(payload);
                break;
            case INT_UNSIGNED:
                columnValue = PacketUtils.readInt4AsLong(payload);
                break;
            case BIGINT:
                columnValue = PacketUtils.readInt8(payload);
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
                columnValue = readBinaryTime(payload);
                break;
            case CHAR:
            case VARCHAR:
            case ENUM:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
                columnValue = PacketUtils.readStringLenEnc(payload, columnCharset);
                break;
            case LONGTEXT:
            case JSON: {
                bytes = PacketUtils.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = LongStrings.fromString(new String(bytes, columnCharset));
                }
            }
            break;
            case BIT: {
                bytes = PacketUtils.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    LOG.debug("from server bit:{}", bytes.length);
                    columnValue = MySQLNumberUtils.readLongFromBigEndian(bytes, 0, bytes.length);
                    LOG.debug("from server bit number:{}", columnValue);
                }
            }
            break;
            case SET: {
                bytes = PacketUtils.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = MySQLConvertUtils.convertToSetType(new String(bytes, columnCharset));
                }
            }
            break;
            case SMALLINT:
                columnValue = (short) PacketUtils.readInt2AsInt(payload);
                break;
            case SMALLINT_UNSIGNED:
                columnValue = PacketUtils.readInt2AsInt(payload);
                break;
            case YEAR:
                columnValue = Year.of(PacketUtils.readInt2AsInt(payload));
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
            case FLOAT_UNSIGNED:
                columnValue = Double.longBitsToDouble(PacketUtils.readInt8(payload));
                break;
            case FLOAT:
                columnValue = Float.intBitsToFloat(PacketUtils.readInt4(payload));
                break;
            case DOUBLE_UNSIGNED: {
                bytes = new byte[8];
                payload.readBytes(bytes);
                columnValue = new BigDecimal(new String(bytes, columnCharset));
            }
            break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
                columnValue = PacketUtils.readBytesLenEnc(payload);
                break;
            case LONGBLOB: {
                bytes = PacketUtils.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = LongBinaries.fromArray(bytes);
                }
            }
            break;
            case GEOMETRY: {
                bytes = PacketUtils.readBytesLenEnc(payload);
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
                                , columnMeta.mysqlType, columnMeta.columnAlias));
            default:
                throw MySQLExceptions.createUnknownEnumException(columnMeta.mysqlType);

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
                columnBytes = PacketUtils.getLenEncTotalByteLength(bigPayloadBuffer);
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
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time">ProtocolBinary::MYSQL_TYPE_TIME</a>
     */
    private LocalTime readBinaryTime(ByteBuf byteBuf) {
        byte length = byteBuf.readByte();
        final LocalTime time;
        switch (length) {
            case 8:
                time = LocalTime.of(byteBuf.readByte(), byteBuf.readByte(), byteBuf.readByte());
                break;
            case 12: {
                time = LocalTime.of(byteBuf.readByte(), byteBuf.readByte(), byteBuf.readByte()
                        , PacketUtils.readInt4(byteBuf));
            }
            break;
            case 0:
                time = LocalTime.MIN;
                break;
            default:
                throw MySQLExceptions.createFatalIoException(
                        "Server send binary MYSQL_TYPE_DATE length[%s] error.", length);
        }
        return OffsetTime.of(time, this.adjutant.obtainZoneOffsetDatabase())
                .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                .toLocalTime();
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
                        PacketUtils.readInt2AsInt(payload) // year
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
                        PacketUtils.readInt2AsInt(payload) // year
                        , payload.readByte()           // month
                        , payload.readByte()           // day

                        , payload.readByte()           // hour
                        , payload.readByte()           // minute
                        , payload.readByte()           // second

                        , PacketUtils.readInt4(payload) * 1000 // micro second
                );
            }
            break;
            case 4: {
                LocalDate date = LocalDate.of(PacketUtils.readInt2AsInt(payload), payload.readByte(), payload.readByte());
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
                date = LocalDate.of(PacketUtils.readInt2AsInt(payload), payload.readByte(), payload.readByte());
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
