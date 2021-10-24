package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

final class TextResultSetReader extends AbstractResultSetReader {

    static TextResultSetReader create(StmtTask task) {
        return new TextResultSetReader(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(TextResultSetReader.class);


    private TextResultSetReader(StmtTask task) {
        super(task);
    }


    @Override
    boolean readResultSetMeta(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer) {

        if ((this.negotiatedCapability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA");
        }

        final boolean endOfMeta = (this.negotiatedCapability & Capabilities.CLIENT_DEPRECATE_EOF) == 0;
        final boolean metaEnd;
        if (MySQLRowMeta.canReadMeta(cumulateBuffer, endOfMeta)) {
            doReadRowMeta(cumulateBuffer);
            if (endOfMeta) {
                final int payloadLength = Packets.readInt3(cumulateBuffer);
                this.task.updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));

                final EofPacket eof;
                eof = EofPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatesConsumer.accept(eof);
            }
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
    boolean isBinaryReader() {
        return false;
    }

    @Override
    ResultRow readOneRow(final ByteBuf cumulateBuffer, final MySQLRowMeta rowMeta) {
        final MySQLColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;
        final Object[] rowValues = new Object[columnMetaArray.length];

        for (int i = 0; i < columnMetaArray.length; i++) {
            if (Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex()) == Packets.ENC_0) {
                cumulateBuffer.readByte();
                continue;
            }
            rowValues[i] = readColumnValue(cumulateBuffer, columnMetaArray[i]);
        }
        return MySQLResultRow.from(rowValues, rowMeta, this.adjutant);
    }

    @Override
    long obtainColumnBytes(final MySQLColumnMeta columnMeta, final ByteBuf payload) {
        return Packets.getLenEncTotalByteLength(payload);
    }


    @SuppressWarnings("deprecation")
    @Nullable
    @Override
    Object readColumnValue(final ByteBuf cumulateBuffer, final MySQLColumnMeta meta) {

        final Charset columnCharset = this.adjutant.obtainColumnCharset(meta.columnCharset);

        String columnText;
        final Object value;
        switch (meta.sqlType) {
            case TINYINT: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Byte.parseByte(columnText);
                }
            }
            break;
            case TINYINT_UNSIGNED:
            case SMALLINT: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Short.parseShort(columnText);
                }
            }
            break;
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Integer.parseInt(columnText);
                }
            }
            break;
            case INT_UNSIGNED:
            case BIGINT: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Long.parseLong(columnText);
                }
            }
            break;
            case BIGINT_UNSIGNED: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = new BigInteger(columnText);
                }
            }
            break;
            case DECIMAL_UNSIGNED:
            case DECIMAL: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                value = columnText == null ? null : new BigDecimal(columnText);
            }
            break;
            case FLOAT_UNSIGNED:
            case FLOAT: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Float.parseFloat(columnText);
                }
            }
            break;
            case DOUBLE_UNSIGNED:
            case DOUBLE: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Double.parseDouble(columnText);
                }
            }
            break;
            case BOOLEAN: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Byte.parseByte(columnText) != 0;
                }
            }
            break;
            case BIT: {
                value = readBitType(cumulateBuffer);
            }
            break;
            case TIME: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else if (MySQLTimes.isDuration(columnText)) {
                    value = MySQLTimes.parseTimeAsDuration(columnText);
                } else {
                    value = LocalTime.parse(columnText, MySQLTimes.ISO_LOCAL_TIME_FORMATTER);
                }
            }
            break;
            case DATE: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else if (columnText.equals("0000-00-00")) {
                    //TODO write test use case
                    value = handleZeroDateBehavior("DATE");
                } else {
                    value = LocalDate.parse(columnText, DateTimeFormatter.ISO_LOCAL_DATE);
                }
            }
            break;
            case YEAR: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = Year.of(Integer.parseInt(columnText));
                }
            }
            break;
            case TIMESTAMP:
            case DATETIME: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else if (columnText.startsWith("0000-00-00")) {
                    LocalDate date = handleZeroDateBehavior("DATETIME");
                    if (date == null) {
                        value = null;
                    } else {
                        LocalTime time = LocalTime.parse(columnText.substring(11), MySQLTimes.MYSQL_TIME_FORMATTER);
                        value = LocalDateTime.of(date, time);
                    }
                } else {
                    value = LocalDateTime.parse(columnText, MySQLTimes.ISO_LOCAL_DATETIME_FORMATTER);
                }
            }
            break;
            case CHAR:
            case VARCHAR:
            case ENUM:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT: {
                value = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
            }
            break;
            case JSON:
            case LONGTEXT: {
                columnText = Packets.readStringLenEnc(cumulateBuffer, columnCharset);
                if (columnText == null) {
                    value = null;
                } else {
                    value = LongStrings.fromString(columnText);
                }
            }
            break;
            case SET: {
                value = readSetType(cumulateBuffer, columnCharset);
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
                final byte[] array = Packets.readBytesLenEnc(cumulateBuffer);
                if (array == null) {
                    value = null;
                } else {
                    value = LongBinaries.fromArray(array);
                }
            }
            break;
            case GEOMETRY: {
                value = readGeometry(cumulateBuffer);
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



    /*################################## blow private method ##################################*/


}
