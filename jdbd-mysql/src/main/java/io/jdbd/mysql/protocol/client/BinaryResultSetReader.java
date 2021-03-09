package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
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
            byteIndex = (i + 2) / 8;
            bitIndex = (i + 2) % 8;
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
        final Object columnValue;
        switch (columnMeta.typeFlag) {
            case ProtocolConstants.TYPE_STRING:
            case ProtocolConstants.TYPE_VARCHAR:
            case ProtocolConstants.TYPE_VAR_STRING:
            case ProtocolConstants.TYPE_TINY_BLOB:
            case ProtocolConstants.TYPE_BLOB:
            case ProtocolConstants.TYPE_MEDIUM_BLOB:
            case ProtocolConstants.TYPE_LONG_BLOB:
            case ProtocolConstants.TYPE_GEOMETRY:
            case ProtocolConstants.TYPE_JSON: {
                columnValue = PacketUtils.readBytesLenEnc(payload);
            }
            break;
            case ProtocolConstants.TYPE_NEWDECIMAL:
            case ProtocolConstants.TYPE_DECIMAL: {
                byte[] bytes = PacketUtils.readBytesLenEnc(payload);
                columnValue = new BigDecimal(new String(bytes, columnMeta.columnCharset));
            }
            break;
            case ProtocolConstants.TYPE_BIT: {
                columnValue = PacketUtils.readBinaryBitTypeAsLong(PacketUtils.readBytesLenEnc(payload));
            }
            break;
            case ProtocolConstants.TYPE_ENUM: {
                columnValue = new String(PacketUtils.readBytesLenEnc(payload), columnMeta.columnCharset);
            }
            break;
            case ProtocolConstants.TYPE_SET: {
                columnValue = MySQLConvertUtils.convertToSetType(
                        new String(PacketUtils.readBytesLenEnc(payload), columnMeta.columnCharset));
            }
            break;
            case ProtocolConstants.TYPE_LONGLONG: {
                if (columnMeta.isUnsigned()) {
                    columnValue = PacketUtils.readInt8AsBigInteger(payload);
                } else {
                    columnValue = PacketUtils.readInt8(payload);
                }
            }
            break;
            case ProtocolConstants.TYPE_LONG: {
                if (columnMeta.isUnsigned()) {
                    columnValue = PacketUtils.readInt4AsLong(payload);
                } else {
                    columnValue = PacketUtils.readInt4(payload);
                }
            }
            break;
            case ProtocolConstants.TYPE_INT24: {
                columnValue = PacketUtils.readInt4(payload);
            }
            break;
            case ProtocolConstants.TYPE_SHORT: {
                if (columnMeta.isUnsigned()) {
                    columnValue = PacketUtils.readInt2(payload);
                } else {
                    columnValue = (short) PacketUtils.readInt2(payload);
                }
            }
            break;
            case ProtocolConstants.TYPE_YEAR: {
                columnValue = Year.of(PacketUtils.readInt2(payload));
            }
            break;
            case ProtocolConstants.TYPE_BOOL: {
                columnValue = MySQLConvertUtils.tryConvertToBoolean(payload.readByte());
            }
            break;
            case ProtocolConstants.TYPE_TINY: {
                switch (columnMeta.mysqlType) {
                    case BIT: {
                        columnValue = (long) payload.readByte();
                    }
                    break;
                    case BOOLEAN: {
                        columnValue = MySQLConvertUtils.tryConvertToBoolean(payload.readByte());
                    }
                    break;
                    case TINYINT_UNSIGNED: {
                        columnValue = (short) PacketUtils.readInt1(payload);
                    }
                    break;
                    default: {
                        columnValue = payload.readByte();
                    }

                }
            }
            break;
            case ProtocolConstants.TYPE_DOUBLE: {
                columnValue = Double.longBitsToDouble(PacketUtils.readInt8(payload));
            }
            break;
            case ProtocolConstants.TYPE_FLOAT: {
                columnValue = Float.intBitsToFloat(PacketUtils.readInt4(payload));
            }
            break;
            case ProtocolConstants.TYPE_DATE: {
                LocalDate date = PacketUtils.readBinaryDate(payload);
                if (date == null) {
                    date = handleZeroDateBehavior();
                    // maybe null
                }
                columnValue = date;
            }
            break;
            case ProtocolConstants.TYPE_TIMESTAMP:
            case ProtocolConstants.TYPE_DATETIME: {
                LocalDateTime dateTime = PacketUtils.readBinaryDateTime(payload, this.adjutant);
                if (dateTime == null) {
                    LocalDate date = handleZeroDateBehavior();
                    if (date != null) {
                        dateTime = LocalDateTime.of(date, LocalTime.MIN);
                    }
                }
                columnValue = dateTime;
            }
            break;
            case ProtocolConstants.TYPE_TIME: {
                columnValue = PacketUtils.readBinaryTime(payload, this.adjutant);
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("Server send unknown type[%s]"
                        , columnMeta.typeFlag);

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


}
