package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.function.Consumer;

final class TextResultSetReader extends AbstractResultSetReader {

    private static final Logger LOG = LoggerFactory.getLogger(TextResultSetReader.class);

    TextResultSetReader(ResultSetReaderBuilder builder) {
        super(builder);
    }


    @Override
    final boolean isResettable() {
        return false;
    }

    @Override
    boolean readResultSetMeta(final ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {

        final int negotiatedCapability = this.adjutant.obtainNegotiatedCapability();
        if ((negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA");
        }
        boolean metaEnd;
        metaEnd = doReadRowMeta(cumulateBuffer);

        if (metaEnd && (negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            if (PacketUtils.hasOnePacket(cumulateBuffer)) {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
                EofPacket eof = EofPacket.read(cumulateBuffer.readSlice(payloadLength), negotiatedCapability);
                serverStatusConsumer.accept(eof);
            } else {
                metaEnd = false;
            }
        }
        return metaEnd;
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    @Override
    int skipNullColumn(BigRowData bigRowData, final ByteBuf payload, final int columnIndex) {
        int i = columnIndex;
        final MySQLColumnMeta[] columnMetaArray = this.rowMeta.columnMetaArray;
        for (; i < columnMetaArray.length; i++) {
            if (PacketUtils.getInt1(payload, payload.readerIndex()) != PacketUtils.ENC_0) {
                break;
            }
            payload.readByte();
        }
        return i;
    }

    @Override
    ResultRow readOneRow(final ByteBuf payload) {
        final MySQLRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
        final MySQLColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;
        final Object[] rowValues = new Object[columnMetaArray.length];

        for (int i = 0; i < columnMetaArray.length; i++) {
            if (PacketUtils.getInt1(payload, payload.readerIndex()) == PacketUtils.ENC_0) {
                payload.readByte();
                continue;
            }
            rowValues[i] = readColumnValue(payload, columnMetaArray[i]);
        }
        return MySQLResultRow.from(rowValues, rowMeta, this.adjutant);
    }

    @Override
    long obtainColumnBytes(final MySQLColumnMeta columnMeta, final ByteBuf bigPayloadBuffer) {
        return PacketUtils.getLenEncTotalByteLength(bigPayloadBuffer);
    }


    @Nullable
    @Override
    Object internalReadColumnValue(final ByteBuf payload, final MySQLColumnMeta columnMeta) {

        final Charset columnCharset = this.adjutant.obtainColumnCharset(columnMeta.columnCharset);

        String columnText;
        final Object columnValue;
        switch (columnMeta.typeFlag) {

            case ProtocolConstants.TYPE_TINY_BLOB:
            case ProtocolConstants.TYPE_BLOB:
            case ProtocolConstants.TYPE_MEDIUM_BLOB:
            case ProtocolConstants.TYPE_LONG_BLOB:
            case ProtocolConstants.TYPE_GEOMETRY: {
                switch (columnMeta.mysqlType) {
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                    case LONGTEXT: {
                        columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                        if (columnText != null && columnMeta.mysqlType == MySQLType.LONGTEXT) {
                            columnValue = new StringReader(columnText);
                        } else {
                            columnValue = columnText;
                        }
                    }
                    break;
                    default:
                        columnValue = PacketUtils.readBytesLenEnc(payload);
                }

            }
            break;
            case ProtocolConstants.TYPE_STRING:
            case ProtocolConstants.TYPE_VARCHAR:
            case ProtocolConstants.TYPE_VAR_STRING: {
                switch (columnMeta.mysqlType) {
                    case BINARY:
                    case VARBINARY:
                        columnValue = PacketUtils.readBytesLenEnc(payload);
                        break;
                    case CHAR:
                    case VARCHAR:
                        columnValue = PacketUtils.readStringLenEnc(payload, columnCharset);
                        break;
                    default:
                        throw new IllegalStateException(
                                String.format("Not found MySQL type for column meta:%s", columnMeta));
                }
            }
            break;
            case ProtocolConstants.TYPE_ENUM:
            case ProtocolConstants.TYPE_JSON: {
                columnValue = PacketUtils.readStringLenEnc(payload, columnCharset);
            }
            break;
            case ProtocolConstants.TYPE_NEWDECIMAL:
            case ProtocolConstants.TYPE_DECIMAL: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                columnValue = columnText == null ? null : new BigDecimal(columnText);
            }
            break;
            case ProtocolConstants.TYPE_BIT: {
                byte[] bytes = PacketUtils.readBytesLenEnc(payload);
                if (bytes == null) {
                    columnValue = null;
                } else {
                    columnValue = PacketUtils.readBitTypeAsLong(bytes);
                }
            }
            break;
            case ProtocolConstants.TYPE_SET: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = MySQLConvertUtils.convertToSetType(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_LONGLONG: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.isUnsigned()) {
                    columnValue = new BigInteger(columnText);
                } else {
                    columnValue = Long.parseLong(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_LONG: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.isUnsigned()) {
                    columnValue = Long.parseLong(columnText);
                } else {
                    columnValue = Integer.parseInt(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_INT24: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = Integer.parseInt(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_SHORT: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.isUnsigned()) {
                    columnValue = Integer.parseInt(columnText);
                } else {
                    columnValue = Short.parseShort(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_YEAR: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = Year.of(Integer.parseInt(columnText));
                }
            }
            break;
            case ProtocolConstants.TYPE_BOOL: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else {
                    columnValue = MySQLConvertUtils.convertObjectToBoolean(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_TINY: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                final boolean bitIsBoolean = columnMeta.length == 1L
                        && this.properties.getOrDefault(PropertyKey.tinyInt1isBit, Boolean.class)
                        && this.properties.getOrDefault(PropertyKey.transformedBitIsBoolean, Boolean.class);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.isUnsigned()) {
                    columnValue = Integer.parseInt(columnText);
                } else {
                    if (bitIsBoolean) {
                        columnValue = MySQLConvertUtils.tryConvertToBoolean(Byte.parseByte(columnText));
                    } else {
                        columnValue = Byte.parseByte(columnText);
                    }
                }
            }
            break;
            case ProtocolConstants.TYPE_DOUBLE: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.isUnsigned()) {
                    columnValue = new BigDecimal(columnText);
                } else {
                    columnValue = Double.parseDouble(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_FLOAT: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnMeta.isUnsigned()) {
                    columnValue = Double.parseDouble(columnText);
                } else {
                    columnValue = Float.parseFloat(columnText);
                }
            }
            break;
            case ProtocolConstants.TYPE_DATE: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnText.equals("0000-00-00")) {
                    columnValue = handleZeroDateBehavior();
                } else {
                    columnValue = LocalDate.parse(columnText, DateTimeFormatter.ISO_LOCAL_DATE);
                }
            }
            break;
            case ProtocolConstants.TYPE_TIMESTAMP:
            case ProtocolConstants.TYPE_DATETIME: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (columnText.startsWith("0000-00-00")) {
                    columnValue = handleZeroDateBehavior();
                } else {
                    LocalDateTime dateTime = LocalDateTime.parse(columnText, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
                    columnValue = OffsetDateTime.of(dateTime, this.adjutant.obtainZoneOffsetDatabase())
                            .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                            .toLocalDateTime();
                }
            }
            break;
            case ProtocolConstants.TYPE_TIME: {
                columnText = PacketUtils.readStringLenEnc(payload, columnCharset);
                if (columnText == null) {
                    columnValue = null;
                } else if (MySQLTimeUtils.isDuration(columnText)) {
                    columnValue = MySQLTimeUtils.parseTimeAsDuration(columnText);
                } else {
                    LocalTime time = LocalTime.parse(columnText, MySQLTimeUtils.MYSQL_TIME_FORMATTER);
                    columnValue = OffsetTime.of(time, this.adjutant.obtainZoneOffsetDatabase())
                            .withOffsetSameInstant(this.adjutant.obtainZoneOffsetClient())
                            .toLocalTime();
                }
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
        return false;
    }

    /*################################## blow private method ##################################*/


}
