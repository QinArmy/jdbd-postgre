package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLTimes;
import io.jdbd.result.ResultRow;
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

final class TextResultSetReader extends MySQLResultSetReader {

    static TextResultSetReader create(StmtTask task) {
        return new TextResultSetReader(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(TextResultSetReader.class);


    private TextResultSetReader(StmtTask task) {
        super(task);
    }


    @Override
    public States read(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer) throws JdbdException {
        return null;
    }

    @Override
    boolean readResultSetMeta(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer) {

        if ((this.capability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
            throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA");
        }

        final boolean endOfMeta = (this.capability & Capabilities.CLIENT_DEPRECATE_EOF) == 0;
        final boolean metaEnd;
        if (MySQLRowMeta.canReadMeta(cumulateBuffer, endOfMeta)) {
            doReadRowMeta(cumulateBuffer);
            if (endOfMeta) {
                final int payloadLength = Packets.readInt3(cumulateBuffer);
                this.task.updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));

                final EofPacket eof;
                eof = EofPacket.read(cumulateBuffer.readSlice(payloadLength), this.capability);
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
        return MySQLResultRow0.from(rowValues, rowMeta, this.adjutant);
    }

    @Override
    long obtainColumnBytes(final MySQLColumnMeta columnMeta, final ByteBuf payload) {
        return Packets.getLenEncTotalByteLength(payload);
    }


    @SuppressWarnings("deprecation")
    @Nullable
    Object readOneColumn(final ByteBuf payload, final int readableBytes, final MySQLColumnMeta meta,
                         final TextCurrentRow currentRow) {


        String columnText;
        final Object value;
        final byte[] bytes;
        switch (meta.sqlType) {
            case NULL:
                throw MySQLExceptions.createFatalIoException("server text protocol return null type", null);
            case LONGTEXT:
            case JSON:
                value = readLongText(payload, meta, currentRow);
                break;
            case GEOMETRY:
                value = readGeometry(payload, meta, currentRow);
                break;
            case LONGBLOB:
            case UNKNOWN:
                value = readLongBlob(payload, meta, currentRow);
                break;
            default: {
                final int lenEnc;
                if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                    break;
                }
                bytes = new byte[lenEnc];
                payload.readBytes(bytes);

                final Charset columnCharset;
                columnCharset = this.adjutant.columnCharset(meta.columnCharset);

                switch (meta.sqlType) {
                    case BOOLEAN:
                        value = Byte.parseByte(new String(bytes, columnCharset)) != 0;
                        break;
                    case TINYINT:
                        value = Byte.parseByte(new String(bytes, columnCharset));
                        break;
                    case TINYINT_UNSIGNED:
                    case SMALLINT:
                        value = Short.parseShort(new String(bytes, columnCharset));
                        break;
                    case SMALLINT_UNSIGNED:
                    case MEDIUMINT:
                    case MEDIUMINT_UNSIGNED:
                    case INT:
                        value = Integer.parseInt(new String(bytes, columnCharset));
                        break;
                    case INT_UNSIGNED:
                    case BIGINT:
                        value = Long.parseLong(new String(bytes, columnCharset));
                        break;
                    case BIGINT_UNSIGNED:
                        value = new BigInteger(new String(bytes, columnCharset));
                        break;
                    case DECIMAL_UNSIGNED:
                    case DECIMAL:
                        value = new BigDecimal(new String(bytes, columnCharset));
                        break;
                    case FLOAT_UNSIGNED:
                    case FLOAT:
                        value = Float.parseFloat(new String(bytes, columnCharset));
                        break;
                    case DOUBLE_UNSIGNED:
                    case DOUBLE:
                        value = Double.parseDouble(new String(bytes, columnCharset));
                        break;
                    case CHAR:
                    case VARCHAR:
                    case ENUM:
                    case SET:
                    case TINYTEXT:
                    case TEXT:
                    case MEDIUMTEXT:
                        value = new String(bytes, columnCharset);
                        break;
                    case BIT:
                        value = parseBitAsLong(bytes);
                        break;
                    case TIME:
                        value = LocalTime.parse(new String(bytes, columnCharset));
                        break;
                    case DATE: {
                        columnText = new String(bytes, columnCharset);
                        if (columnText.equals("0000-00-00")) {
                            value = handleZeroDateBehavior("DATE");
                        } else {
                            value = LocalDate.parse(columnText, DateTimeFormatter.ISO_LOCAL_DATE);
                        }
                    }
                    break;
                    case YEAR:
                        value = Year.of(Integer.parseInt(new String(bytes, columnCharset)));
                        break;
                    case TIMESTAMP:
                    case DATETIME: {
                        columnText = new String(bytes, columnCharset);
                        final LocalDate date;
                        if (!columnText.startsWith("0000-00-00")) {
                            value = LocalDateTime.parse(columnText, MySQLTimes.DATETIME_FORMATTER_6);
                        } else if ((date = handleZeroDateBehavior("DATETIME")) == null) {
                            value = null;
                        } else {
                            LocalTime time = LocalTime.parse(columnText.substring(11), MySQLTimes.TIME_FORMATTER_6);
                            value = LocalDateTime.of(date, time);
                        }
                    }
                    break;
                    case BINARY:
                    case VARBINARY:
                    case TINYBLOB:
                    case BLOB:
                    case MEDIUMBLOB:
                        value = bytes;
                        break;
                    default:
                        throw MySQLExceptions.unexpectedEnum(meta.sqlType);

                } // inter switch

            } // outer switch default

        } // outer switch


        return value;
    }



    /*################################## blow private method ##################################*/


    private static final class TextCurrentRow extends MySQLCurrentRow {

        private int columnIndex = 0;

        private long rowCount = 0L;

        private TextCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
        }

        @Override
        public long rowNumber() {
            return this.rowCount;
        }


        @Override
        void doRest() {
            if (this.columnIndex != this.columnArray.length) {
                throw new IllegalStateException();
            }
            this.columnIndex = 0;
        }


    }//BinaryCurrentRow


}
