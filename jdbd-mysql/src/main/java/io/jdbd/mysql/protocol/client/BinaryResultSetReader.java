package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.util.MySQLExceptions;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.*;
import java.util.function.Consumer;


/**
 * @see ComPreparedTask
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html">Binary Protocol Resultset</a>
 */
final class BinaryResultSetReader extends MySQLResultSetReader {

    static BinaryResultSetReader create(StmtTask task) {
        return new BinaryResultSetReader(task);
    }

    private static final Logger LOG = LoggerFactory.getLogger(BinaryResultSetReader.class);

    private static final byte BINARY_ROW_HEADER = 0x00;


    private BinaryCurrentRow currentRow;


    private ByteBuf bigPayload;


    private BinaryResultSetReader(StmtTask task) {
        super(task);
    }


    @Override
    public States read(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer)
            throws JdbdException {
        BinaryCurrentRow currentRow = this.currentRow;

        if (currentRow == null && MySQLRowMeta.canReadMeta(cumulateBuffer, false)) {
            currentRow = new BinaryCurrentRow(MySQLRowMeta.readForRows(cumulateBuffer, this.task));
            this.currentRow = currentRow;
        }

        final States states;
        if (currentRow == null) {
            states = States.MORE_CUMULATE;
        } else {
            states = readRows(cumulateBuffer, serverStatesConsumer);
        }
        return states;
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


    private States readRows(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer) {
        final BinaryCurrentRow currentRow = this.currentRow;
        assert currentRow != null;
        final StmtTask task = this.task;


        States states = States.MORE_CUMULATE;
        ByteBuf payload;
        int sequenceId = -1;

        ByteBuf bigPayload = this.bigPayload;
        boolean resultSetEnd = false, oneRowEnd, cancelled;
        cancelled = task.isCancelled();

        for (int payloadLength, packetIndex; Packets.hasOnePacket(cumulateBuffer); ) {
            packetIndex = cumulateBuffer.readerIndex();
            payloadLength = Packets.readInt3(cumulateBuffer);

            if (Packets.getInt1AsInt(cumulateBuffer, packetIndex + Packets.HEADER_SIZE) == EofPacket.EOF_HEADER) {
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);

                final TerminatorPacket terminator;
                terminator = TerminatorPacket.fromCumulate(cumulateBuffer, payloadLength, this.capability);
                this.currentRow = null;
                resultSetEnd = true;
                serverStatesConsumer.accept(terminator);

                if (!cancelled) {
                    task.next(MySQLResultStates.fromQuery(currentRow.getResultIndex(), terminator, currentRow.rowCount));
                }
                if (terminator.hasMoreFetch()) {
                    states = States.MORE_FETCH;
                } else if (terminator.hasMoreResult()) {
                    states = States.MORE_RESULT;
                } else {
                    states = States.NO_MORE_RESULT;
                }
                break;
            }

            if (cancelled) {
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                cumulateBuffer.skipBytes(payloadLength);
                continue;
            }

            if (bigPayload != null && payloadLength < Packets.MAX_PAYLOAD) {
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                payload = cumulateBuffer;
            } else {
                if (bigPayload == null) {
                    this.bigPayload = bigPayload = this.adjutant.allocator()
                            .buffer(Packets.payloadBytesOfBigPacket(cumulateBuffer), Integer.MAX_VALUE);
                }
                payload = bigPayload;
                sequenceId = Packets.readBigPayload(cumulateBuffer, payload);
            }

            oneRowEnd = readOneRow(payload, payload != cumulateBuffer, currentRow);

            if (payload == cumulateBuffer) {
                assert oneRowEnd; // fail ,driver bug or server bug
                cumulateBuffer.readerIndex(packetIndex + Packets.HEADER_SIZE + packetIndex); //avoid tailor filler
            } else if (oneRowEnd) {
                assert payload.readableBytes() == 0; // fail , driver bug.
                payload.release();
                this.bigPayload = bigPayload = null;
            } else if (payload.readableBytes() == 0) {
                payload.readerIndex(0);
                payload.writerIndex(0);
            } else if (payload.readerIndex() > 0) {
                payload.discardReadBytes();
            }

            if (oneRowEnd) {
                // MORE_CUMULATE
                break;
            }

            if (!(cancelled = this.error != null)) {
                currentRow.rowCount++;
                task.next(currentRow);
                currentRow.reset();
            }
        }

        if (sequenceId > -1) {
            this.task.updateSequenceId(sequenceId);
        }

        if (resultSetEnd) {
            bigPayload = this.bigPayload;
            if (bigPayload != null && bigPayload.refCnt() > 0) {
                bigPayload.release();
            }
            this.bigPayload = null;
        }
        return states;
    }


    /**
     * @return true : one row end.
     */
    private boolean readOneRow(final ByteBuf payload, final boolean bigRow, final BinaryCurrentRow currentRow) {
        final MySQLColumnMeta[] metaArray = currentRow.rowMeta.columnMetaArray;
        if (payload.readByte() != BINARY_ROW_HEADER) {
            throw new IllegalArgumentException("cumulateBuffer isn't binary row");
        }
        final Object[] columnValues = currentRow.columnArray;
        final byte[] nullBitMap = currentRow.nullBitMap;

        MySQLColumnMeta columnMeta;
        int columnIndex = currentRow.columnIndex;

        if (columnIndex == 0 && !currentRow.readNullBitMap) {
            payload.readBytes(nullBitMap); // null_bitmap
            currentRow.readNullBitMap = true; // avoid big row
        }
        Object value;
        for (int byteIndex, bitIndex; columnIndex < metaArray.length; columnIndex++) {
            columnMeta = metaArray[columnIndex];
            byteIndex = (columnIndex + 2) & (~7);
            bitIndex = (columnIndex + 2) & 7;
            if ((nullBitMap[byteIndex] & (1 << bitIndex)) != 0) {
                columnValues[columnIndex] = null;
                continue;
            }
            value = readOneColumn(payload, bigRow, columnMeta, currentRow);
            if (value == MORE_CUMULATE_OBJECT) {
                break;
            }
            columnValues[columnIndex] = value;
        }
        currentRow.columnIndex = columnIndex;
        return columnIndex == metaArray.length;
    }


    /**
     * @return <ul>
     * <li>null : {@code DATETIME} is zero.</li>
     * <li>{@link #MORE_CUMULATE_OBJECT} more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    @SuppressWarnings("deprecation")
    @Nullable
    private Object readOneColumn(final ByteBuf payload, final boolean bigRow, final MySQLColumnMeta meta,
                                 final BinaryCurrentRow currentRow) {
        final int readableBytes;
        if (bigRow) {
            readableBytes = Integer.MAX_VALUE;
        } else {
            readableBytes = payload.readableBytes();
        }
        final Object value;
        final byte[] bytes;
        switch (meta.sqlType) {
            case TINYINT: {
                if (readableBytes > 0) {
                    value = payload.readByte();
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case TINYINT_UNSIGNED: {
                if (readableBytes > 0) {
                    value = (short) (payload.readByte() & 0xFF);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case SMALLINT: {
                if (readableBytes > 1) {
                    value = (short) Packets.readInt2AsInt(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case SMALLINT_UNSIGNED: {
                if (readableBytes > 1) {
                    value = Packets.readInt2AsInt(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case MEDIUMINT: {
                final int v;
                if (readableBytes < 3) {
                    value = MORE_CUMULATE_OBJECT;
                } else if (((v = Packets.readInt3(payload)) & 0x80_00_00) == 0) {//positive
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
                if (readableBytes > 2) {
                    value = Packets.readInt3(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case INT: {
                if (readableBytes > 3) {
                    value = Packets.readInt4(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case INT_UNSIGNED: {
                if (readableBytes > 3) {
                    value = Packets.readInt4AsLong(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BIGINT: {
                if (readableBytes > 7) {
                    value = Packets.readInt8(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BIGINT_UNSIGNED: {
                if (readableBytes > 7) {
                    value = Packets.readInt8AsBigInteger(payload);
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                final int lenEnc;
                if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    bytes = new byte[lenEnc];
                    payload.readBytes(bytes);
                    value = new BigDecimal(new String(bytes, this.adjutant.columnCharset(meta.columnCharset)));
                }
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                if (readableBytes > 3) {
                    value = Float.intBitsToFloat(Packets.readInt4(payload));
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                if (readableBytes > 7) {
                    value = Double.longBitsToDouble(Packets.readInt8(payload));
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BOOLEAN: {
                if (readableBytes > 0) {
                    value = payload.readByte() != 0;
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case BIT:
                value = readBitType(payload, readableBytes);
                break;
            case TIME:
                value = readBinaryTimeType(payload, readableBytes);
                break;
            case DATE:
                value = readBinaryDate(payload, readableBytes);
                break;
            case YEAR: {
                if (readableBytes > 1) {
                    value = Year.of(Packets.readInt2AsInt(payload));
                } else {
                    value = MORE_CUMULATE_OBJECT;
                }
            }
            break;
            case TIMESTAMP:
            case DATETIME: {
                if (readableBytes == 0 || payload.getByte(payload.readerIndex()) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    value = readBinaryDateTime(payload);
                }
            }
            break;
            case CHAR:
            case VARCHAR:
            case ENUM:
            case SET:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT: {
                final int lenEnc;
                if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    bytes = new byte[lenEnc];
                    payload.readBytes(bytes);
                    value = new String(bytes, this.adjutant.columnCharset(meta.columnCharset));
                }
            }
            break;
            case LONGTEXT:
            case JSON: // handle JSON as long text
                value = readLongText(payload, meta, currentRow);
                break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB: {
                final int lenEnc;
                if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(payload)) > readableBytes) {
                    value = MORE_CUMULATE_OBJECT;
                } else {
                    bytes = new byte[lenEnc];
                    payload.readBytes(bytes);
                    value = bytes;
                }
            }
            break;
            case GEOMETRY:
                value = readGeometry(payload, meta, currentRow);
                break;
            case NULL:
                throw MySQLExceptions.createFatalIoException("server return null type", null);
            case LONGBLOB:
            case UNKNOWN:
            default: // handle unknown as long blob.
                value = readLongBlob(payload, meta, currentRow);

        } //switch
        return value;
    }


    @Override
    boolean isBinaryReader() {
        return true;
    }



    /*################################## blow private method ##################################*/


    /**
     * @return {@link LocalTime} or {@link Duration}
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value_time">ProtocolBinary::MYSQL_TYPE_TIME</a>
     */
    private Object readBinaryTimeType(final ByteBuf byteBuf, final int readableBytes) {
        if (readableBytes == 0) {
            return MORE_CUMULATE_OBJECT;
        }
        final int length = byteBuf.readByte();
        if (length == 0) {
            return LocalTime.MIDNIGHT;
        } else if (length > readableBytes) {
            byteBuf.readerIndex(byteBuf.readerIndex() - 1);
            return MORE_CUMULATE_OBJECT;
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
    private LocalDateTime readBinaryDateTime(final ByteBuf payload) {

        final int length = payload.readByte();
        LocalDateTime dateTime;
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
                        String.format("Server send binary MYSQL_TYPE_DATETIME length[%s] error.", length), null);
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
    private Object readBinaryDate(final ByteBuf payload, final int readableBytes) {
        if (readableBytes == 0) {
            return MORE_CUMULATE_OBJECT;
        }
        final int length = payload.readByte();
        if (length > readableBytes) {
            payload.readerIndex(payload.readerIndex() - 1);
            return MORE_CUMULATE_OBJECT;
        }
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
                        String.format("Server send binary MYSQL_TYPE_DATE length[%s] error.", length), null);
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


    private static final class BinaryCurrentRow extends MySQLCurrentRow {


        private final byte[] nullBitMap;

        private int columnIndex = 0;

        private boolean readNullBitMap;

        private long rowCount = 0L;

        private BinaryCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
            this.nullBitMap = new byte[(rowMeta.columnMetaArray.length + 9) >> 3];
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
            this.readNullBitMap = false;
            this.columnIndex = 0;
        }
    }//BinaryCurrentRow


}
