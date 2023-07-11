package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.env.MySQLEnvironment;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.BigRowIoException;
import io.jdbd.result.ResultRow;
import io.jdbd.type.BlobPath;
import io.jdbd.type.TextPath;
import io.jdbd.vendor.env.Properties;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

abstract class MySQLResultSetReader implements ResultSetReader {

    static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/mysql/bigRow")
            .toAbsolutePath();

    private static final int BIG_COLUMN_BOUND = Integer.MAX_VALUE - (4 << 7);

    static final Object MORE_CUMULATE_OBJECT = States.MORE_CUMULATE;

    final TaskAdjutant adjutant;

    final StmtTask task;

    final Properties properties;

    final int capability;

    final FixedEnv fixedEnv;

    final MySQLEnvironment env;

    Throwable error;

    private MySQLRowMeta rowMeta;


    private States states = States.MORE_CUMULATE;

    private boolean resultSetEnd;

    private long readRowCount = 0;

    private Phase phase = Phase.READ_RESULT_META;

    private BigRowData bigRowData;

    private ByteBuf bigPayload;


    private long invokerCount = 0;

    MySQLResultSetReader(StmtTask task) {
        this.task = task;
        this.adjutant = task.adjutant();
        this.properties = this.adjutant.host().getProperties();
        this.capability = this.adjutant.capability();
        this.fixedEnv = this.adjutant.getFactory();
        this.env = this.fixedEnv.env;
    }

    // @Override
    public final States read0(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer)
            throws JdbdException {
        boolean resultSetEnd = this.resultSetEnd;
        if (resultSetEnd) {
            // no bug,never here
            throw new IllegalStateException("ResultSet have ended.");
        }
        try {
            boolean continueRead;
            continueRead = Packets.hasOnePacket(cumulateBuffer);
            while (continueRead) {
                this.invokerCount++;
                switch (this.phase) {
                    case READ_RESULT_META: {
                        if (readResultSetMeta(cumulateBuffer, serverStatesConsumer)) {
                            this.phase = Phase.READ_RESULT_ROW;
                            continueRead = Packets.hasOnePacket(cumulateBuffer);
                        } else {
                            continueRead = false;
                        }
                    }
                    break;
                    case READ_RESULT_ROW: {
                        resultSetEnd = readResultRows(cumulateBuffer, serverStatesConsumer);
                        continueRead = !resultSetEnd && this.phase == Phase.READ_BIG_ROW;
                    }
                    break;
                    case READ_BIG_ROW: {
                        if (readOneBigRow(cumulateBuffer)) {
                            this.phase = Phase.READ_RESULT_ROW;
                            continueRead = Packets.hasOnePacket(cumulateBuffer);
                        } else {
                            continueRead = this.phase == Phase.READ_BIG_COLUMN;
                        }
                    }
                    break;
                    case READ_BIG_COLUMN: {
                        if (readBigColumn(cumulateBuffer)) {
                            this.phase = Phase.READ_BIG_ROW;
                        } else {
                            continueRead = false;
                        }
                    }
                    break;
                    default:
                        throw MySQLExceptions.unexpectedEnum(this.phase);
                }
            }
        } catch (Throwable e) {
            this.resultSetEnd = true;
            releaseOnError();
            throw e;
        }
        final States states = this.states; // store value before reset.
        if (resultSetEnd) {
            if (this.states == States.MORE_CUMULATE) {
                // here bug.
                throw new IllegalStateException("this.states is MORE_CUMULATE");
            }
            resetReader();
        }
        return states;
    }


    /*################################## blow packet template method ##################################*/


    /**
     * @return true: read result set meta end.
     * @see #read(ByteBuf, Consumer)
     */
    abstract boolean readResultSetMeta(ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer);


    ResultRow readOneRow(ByteBuf cumulateBuffer, MySQLRowMeta rowMeta) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return -1 : more cumulate.
     */
    long obtainColumnBytes(MySQLColumnMeta columnMeta, ByteBuf payload) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return maybe null ,only when {@code DATETIME} is zero.
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     */
    @Nullable
    Object readColumnValue(ByteBuf cumulateBuffer, MySQLColumnMeta columnMeta) {
        throw new UnsupportedOperationException();
    }

    abstract boolean isBinaryReader();

    abstract Logger getLogger();


    /*################################## blow final packet method ##################################*/


    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readLongText(final ByteBuf payload, final MySQLColumnMeta meta,
                              final MySQLCurrentRow currentRow) {
        BigColumn bigColumn = currentRow.bigColumn;
        final Object value;
        final int readableBytes;
        readableBytes = payload.readableBytes();

        final long lenEnc;
        final byte[] bytes;
        if (bigColumn != null) {
            if (readBigColumn(payload, bigColumn)) {
                value = TextPath.from(true, this.adjutant.columnCharset(meta.columnCharset), bigColumn.path);
            } else {
                value = MORE_CUMULATE_OBJECT;
            }
        } else if (readableBytes == 0) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.readLenEnc(payload)) <= readableBytes) {
            bytes = new byte[(int) lenEnc];
            payload.readBytes(bytes);
            value = new String(bytes, this.adjutant.columnCharset(meta.columnCharset));
        } else if (lenEnc <= this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc)) == null) {
            payload.skipBytes(payload.readableBytes());
            value = MORE_CUMULATE_OBJECT;
        } else {
            currentRow.setBigColumn(bigColumn);
            readBigColumn(payload, bigColumn);
            value = MORE_CUMULATE_OBJECT;
        }
        return value;
    }

    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readLongBlob(final ByteBuf payload, final MySQLColumnMeta meta, final MySQLCurrentRow currentRow) {
        BigColumn bigColumn = currentRow.bigColumn;
        final Object value;
        final int readableBytes;
        readableBytes = payload.readableBytes();

        final long lenEnc;
        final byte[] bytes;
        if (bigColumn != null) {
            if (readBigColumn(payload, bigColumn)) {
                value = BlobPath.from(true, bigColumn.path);
            } else {
                value = MORE_CUMULATE_OBJECT;
            }
        } else if (readableBytes == 0) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.readLenEnc(payload)) <= readableBytes) {
            bytes = new byte[(int) lenEnc];
            payload.readBytes(bytes);
            value = bytes;
        } else if (lenEnc < this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc)) == null) {
            payload.skipBytes(payload.readableBytes());
            value = MORE_CUMULATE_OBJECT;
        } else {
            currentRow.setBigColumn(bigColumn);
            readBigColumn(payload, bigColumn);
            value = MORE_CUMULATE_OBJECT;
        }
        return value;
    }


    /**
     * @return <ul>
     * <li>{@link #MORE_CUMULATE_OBJECT} : more cumulate</li>
     * <li>column value</li>
     * </ul>
     */
    final Object readGeometry(final ByteBuf payload, final MySQLColumnMeta meta, final MySQLCurrentRow currentRow) {
        BigColumn bigColumn = currentRow.bigColumn;
        final Object value;
        final int readableBytes;
        readableBytes = payload.readableBytes();

        final long lenEnc;
        final byte[] bytes;
        if (bigColumn != null) {
            if (readBigColumn(payload, bigColumn)) {
                value = BlobPath.from(true, bigColumn.path);
            } else {
                value = MORE_CUMULATE_OBJECT;
            }
        } else if (readableBytes < 5) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((lenEnc = Packets.readLenEnc(payload)) <= readableBytes) {
            payload.skipBytes(4);// skip geometry prefix
            bytes = new byte[((int) lenEnc) - 4];
            payload.readBytes(bytes);
            value = bytes;
        } else if (lenEnc < this.fixedEnv.bigColumnBoundaryBytes) {
            value = MORE_CUMULATE_OBJECT;
        } else if ((bigColumn = createBigColumnFile(meta.columnIndex, lenEnc - 4)) == null) {
            payload.skipBytes(payload.readableBytes());
            value = MORE_CUMULATE_OBJECT;
        } else {
            currentRow.setBigColumn(bigColumn);
            payload.skipBytes(4);// skip geometry prefix
            readBigColumn(payload, bigColumn);
            value = MORE_CUMULATE_OBJECT;
        }
        return value;
    }


    final Object readBitType(final ByteBuf cumulateBuffer, final int readableBytes) {
        final int lenEnc;
        if (readableBytes == 0 || (lenEnc = Packets.readLenEncAsInt(cumulateBuffer)) > readableBytes) {
            return MORE_CUMULATE_OBJECT;
        }
        final byte[] bytes;
        bytes = new byte[lenEnc];
        cumulateBuffer.readBytes(bytes);
        return parseBitAsLong(bytes);
    }

    final long parseBitAsLong(final byte[] bytes) {
        long v = 0L;
        for (int i = 0, bits = (bytes.length - 1) << 3; i < bytes.length; i++, bits -= 8) {
            v |= ((bytes[i] & 0xFFL) << bits);
        }
        return v;
    }


    final Object readUnknown(final ByteBuf cumulateBuffer, final int readableByte, final MySQLColumnMeta meta,
                             final Charset columnCharset) {

        final long lenEnc;
        final Object value;
        if (readableByte == 0 || (lenEnc = Packets.readLenEnc(cumulateBuffer)) > readableByte) {
            value = MORE_CUMULATE_OBJECT;
        } else {
            final byte[] bytes = new byte[(int) lenEnc];
            cumulateBuffer.readBytes(bytes);

            if (meta.isBinary()) {
                value = bytes;
            } else {
                value = new String()
            }
        }
        return value;
    }


    /**
     * @return true: read ResultSet end.
     * @see #read(ByteBuf, Consumer)
     */
    private boolean readResultRows(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer) {
        assertPhase(Phase.READ_RESULT_ROW);
        final Logger LOG = getLogger();
        if (LOG.isTraceEnabled()) {
            LOG.trace("read text ResultSet rows");
        }

        final StmtTask sink = this.task;
        final MySQLRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");

        boolean resultSetEnd = false;
        int sequenceId = -1;
        final boolean binaryReader = isBinaryReader();
        final boolean cancelled = sink.isCancelled();
        long readRowCount = this.readRowCount;

        ByteBuf payload = null;
        try {
            outFor:
            for (int payloadLength, header, packetIndex; Packets.hasOnePacket(cumulateBuffer); ) {
                packetIndex = cumulateBuffer.readerIndex();
                payloadLength = Packets.getInt3(cumulateBuffer, packetIndex); // read payload length

                header = Packets.getInt1AsInt(cumulateBuffer, packetIndex + Packets.HEADER_SIZE);
                if (header == ErrorPacket.ERROR_HEADER) {
                    sequenceId = this.readErrorPacket(cumulateBuffer);
                    resultSetEnd = true;
                    break;
                } else if (header == EofPacket.EOF_HEADER && (binaryReader || payloadLength < Packets.MAX_PAYLOAD)) {
                    // this block : read ResultSet end.
                    sequenceId = this.readTerminatePacket(cumulateBuffer, serverStatesConsumer, cancelled);
                    resultSetEnd = true;
                    break;
                }
                if (payloadLength == Packets.MAX_PAYLOAD) {
                    final BigPacketMode mode = readBIgPacket(cumulateBuffer);
                    switch (mode) {
                        case MORE_CUMULATE:
                        case BIG_ROW:
                            break outFor;
                        case BIG_PAYLOAD:
                            payload = Objects.requireNonNull(this.bigPayload, "this.bigPayload");
                            this.bigPayload = null;
                            break;
                        default:
                            throw MySQLExceptions.unexpectedEnum(mode);
                    }
                } else {
                    cumulateBuffer.skipBytes(3); // skip payload length
                    sequenceId = Packets.readInt1AsInt(cumulateBuffer); // read packet sequence_id
                    payload = cumulateBuffer;
                }
                if (cancelled) {
                    if (payload == cumulateBuffer) {
                        cumulateBuffer.skipBytes(payloadLength);
                    } else {
                        payload.release();
                    }
                    continue;
                }
                final int nextPacketIndex = payload.readerIndex() + payloadLength;
                final ResultRow row;
                row = readOneRow(payload, rowMeta);
                readRowCount++;
                sink.next(row);

                if (payload == cumulateBuffer) {
                    if (cumulateBuffer.readerIndex() > nextPacketIndex) {
                        throw new IllegalStateException("readOneRow() method error.");
                    }
                    cumulateBuffer.readerIndex(nextPacketIndex); // avoid tail filler
                } else {
                    payload.release();
                }

            }
        } catch (Throwable e) {
            if (payload != null && payload != cumulateBuffer && payload.refCnt() > 0) {
                payload.release();
            }
            throw e;
        }

        this.readRowCount = readRowCount;
        if (sequenceId > -1) {
            this.task.updateSequenceId(sequenceId);
        }
        return resultSetEnd;
    }


    final void doReadRowMeta(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_RESULT_META);
        if (this.rowMeta != null) {
            throw new IllegalStateException("this.rowMeta is non-null");
        }
        this.rowMeta = MySQLRowMeta.readForRows(cumulateBuffer, this.task);
    }


    @Nullable
    final LocalDate handleZeroDateBehavior(String type) {
        final Enums.ZeroDatetimeBehavior behavior;
        behavior = this.env.getOrDefault(MySQLKey.ZERO_DATE_TIME_BEHAVIOR);
        final LocalDate date;
        switch (behavior) {
            case EXCEPTION: {
                String message = String.format("%s type can't is 0,@see jdbc url property[%s].",
                        type, MySQLKey.ZERO_DATE_TIME_BEHAVIOR);
                final Throwable error;
                error = MySQLExceptions.createTruncatedWrongValue(message, null);
                this.error = error;
                this.task.addErrorToTask(error);
                date = null;
            }
            break;
            case ROUND:
                date = LocalDate.of(1, 1, 1);
                break;
            case CONVERT_TO_NULL:
                date = null;
                break;
            default:
                throw MySQLExceptions.unexpectedEnum(behavior);
        }
        return date;
    }

    @Nullable
    final BigColumn createBigColumnFile(final int columnIndex, final long totalBytes) {
        Path path;
        try {
            path = Files.createTempFile(TEMP_DIRECTORY, "big_column", ".jdbd");
        } catch (Throwable e) {
            path = null;
            this.error = e;
            this.task.addErrorToTask(e);
        }
        if (path == null) {
            return null;
        }
        return new BigColumn(columnIndex, path, totalBytes);
    }


    /**
     * @return true : big column end
     */
    final boolean readBigColumn(final ByteBuf payload, final BigColumn bigColumn) {
        final long restBytes;
        restBytes = bigColumn.restyBytes();

        if (restBytes == 0) {
            // last packet is max payload
            return true;
        }
        final int readableBytes, readLength;
        readableBytes = payload.readableBytes();
        if (restBytes < readableBytes) {
            readLength = (int) restBytes;
        } else {
            readLength = readableBytes;
        }

        if (this.task.isCancelled()) {
            payload.skipBytes(readLength);
            return bigColumn.writeBytes(readLength);
        }

        final int startIndex;
        startIndex = payload.readerIndex();
        Throwable error = null;
        try (FileChannel channel = FileChannel.open(bigColumn.path, StandardOpenOption.APPEND)) {

            payload.readBytes(channel, channel.position(), readLength);

        } catch (Throwable e) {
            this.error = error = e;
            payload.readerIndex(startIndex + readLength);
            this.task.addErrorToTask(e);
        }

        if (error != null) {
            try {
                Files.deleteIfExists(bigColumn.path);
            } catch (Throwable e) {
                this.task.addErrorToTask(e);
            }
        }
        return bigColumn.writeBytes(readLength);
    }




    /*################################## blow private method ##################################*/


    /**
     * @return sequenceId
     */
    private int readErrorPacket(final ByteBuf cumulateBuffer) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        final int sequenceId = Packets.readInt1AsInt(cumulateBuffer);
        final ErrorPacket error;
        error = ErrorPacket.read(cumulateBuffer.readSlice(payloadLength), this.capability
                , this.adjutant.obtainCharsetError());
        this.task.addErrorToTask(MySQLExceptions.createErrorPacketException(error));
        this.resultSetEnd = true;
        this.states = States.END_ON_ERROR;
        return sequenceId;
    }

    /**
     * @return sequenceId
     */
    private int readTerminatePacket(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer
            , final boolean canceled) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        final int sequenceId = Packets.readInt1AsInt(cumulateBuffer);

        final int negotiatedCapability = this.capability;
        final TerminatorPacket tp;
        if (Capabilities.deprecateEof(negotiatedCapability)) {
            tp = OkPacket.read(cumulateBuffer.readSlice(payloadLength), negotiatedCapability);
        } else {
            tp = EofPacket.read(cumulateBuffer.readSlice(payloadLength), negotiatedCapability);
        }
        serverStatesConsumer.accept(tp);
        this.resultSetEnd = true;
        if (tp.hasMoreFetch()) {
            this.states = States.MORE_FETCH;
        } else if (tp.hasMoreResult()) {
            this.states = States.MORE_RESULT;
        } else {
            this.states = States.NO_MORE_RESULT;
        }
        if (!canceled) {
            this.task.next(MySQLResultStates.fromQuery(rowMeta.getResultIndex(), tp, this.readRowCount));
        }
        final Logger LOG = getLogger();
        if (LOG.isTraceEnabled()) {
            LOG.trace("read  ResultSet end.");
        }
        return sequenceId;
    }

    /**
     * @see #read(ByteBuf, Consumer)
     */
    private void releaseOnError() {
        ByteBuf buffer = this.bigPayload;
        if (buffer != null && buffer.refCnt() > 0) {
            buffer.release();
        }
        final BigRowData bigRowData = this.bigRowData;
        if (bigRowData != null) {
            releaseBigRowDataOnError(bigRowData);

        }
    }

    /**
     * @see #releaseOnError()
     */
    private void releaseBigRowDataOnError(final BigRowData bigRowData) {
        ByteBuf buffer;
        if ((buffer = bigRowData.cachePayload) != null && buffer.refCnt() > 0) {
            buffer.release();
        }
        if (bigRowData.index >= bigRowData.values.length) {
            return;
        }
        final Object value = bigRowData.values[bigRowData.index];
        if (value instanceof BigColumn) {
            try {
                Files.deleteIfExists(((BigColumn) value).path);
            } catch (Throwable e) {
                final Logger log = getLogger();
                if (log.isDebugEnabled()) {
                    log.error("delete big column temp file[{}] error.", ((BigColumn) value).path);
                }
            }
        }

        Arrays.fill(bigRowData.values, null);

    }


    /**
     * @see #read(ByteBuf, Consumer)
     */
    private void resetReader() {
        this.phase = Phase.READ_RESULT_META;
        this.readRowCount = 0L;
        this.rowMeta = null;
        this.states = States.MORE_CUMULATE;

        ByteBuf buffer;
        if ((buffer = this.bigPayload) != null && buffer.refCnt() > 0) {
            buffer.release();
        }
        BigRowData bigRowData = this.bigRowData;
        if (bigRowData != null) {
            if ((buffer = bigRowData.cachePayload) != null && buffer.refCnt() > 0) {
                buffer.release();
            }
        }
        this.bigPayload = null;
        this.bigRowData = null;
        this.resultSetEnd = false;
    }


    /**
     * <p>
     * probably modify {@link #phase}
     * </p>
     */
    private BigPacketMode readBIgPacket(final ByteBuf cumulateBuffer) {
        int sequenceId = -1;
        final ByteBuf payload = getBigPayload();

        BigPacketMode mode = BigPacketMode.MORE_CUMULATE;
        for (int payloadLength, packetIndex; Packets.hasOnePacket(cumulateBuffer); ) {
            packetIndex = cumulateBuffer.readerIndex();
            payloadLength = Packets.readInt3(cumulateBuffer);

            if (payload.maxWritableBytes() < payloadLength) {
                cumulateBuffer.readerIndex(packetIndex);
                mode = BigPacketMode.BIG_ROW;
                break;
            }
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);
            payload.writeBytes(cumulateBuffer, payloadLength);
            if (payloadLength < Packets.MAX_PAYLOAD) {
                mode = BigPacketMode.BIG_PAYLOAD;
                break;
            }
        }

        if (sequenceId > -1) {
            this.task.updateSequenceId(sequenceId);
        }

        if (mode == BigPacketMode.BIG_ROW) {
            if (this.bigRowData != null) {
                throw new IllegalStateException("this.bigRowData non-null.");
            }
            final int columnCount = Objects.requireNonNull(this.rowMeta, "this.rowMeta").columnMetaArray.length;
            final byte[] nullBitMap;
            if (isBinaryReader()) {
                nullBitMap = new byte[(columnCount + 9) >> 3];
            } else {
                nullBitMap = BigRowData.EMPTY_BIT_MAP;
            }
            final BigRowData bigRowData = new BigRowData(columnCount, nullBitMap);
            bigRowData.cachePayload = payload;
            this.bigPayload = null;
            this.bigRowData = bigRowData;

            this.phase = Phase.READ_BIG_ROW;
        }
        return mode;
    }


    private ByteBuf getBigPayload() {
        ByteBuf payload = this.bigPayload;
        if (payload == null) {
            payload = this.adjutant.allocator().buffer(Packets.MAX_PAYLOAD << 1, (1 << 30));
            this.bigPayload = payload;
        }
        return payload;
    }


    private ByteBuf mergePayload(final ByteBuf cumulateBuffer, final ByteBuf cache, final BigRowData bigRowData) {
        if (bigRowData.payloadEnd) {
            throw new IllegalArgumentException("big row have ended.");
        }
        int sequenceId = -1;
        ByteBuf payload = cache;
        for (int payloadLength; Packets.hasOnePacket(cumulateBuffer); ) {
            payloadLength = Packets.readInt3(cumulateBuffer);
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);

            if (payload.maxWritableBytes() < payloadLength) {
                ByteBuf temp = this.adjutant.allocator().buffer(payload.readableBytes() + payloadLength, (1 << 30));
                temp.writeBytes(payload);
                payload = temp;
            }
            payload.writeBytes(cumulateBuffer, payloadLength);

            if (payloadLength < Packets.MAX_PAYLOAD) {
                bigRowData.payloadEnd = true;
                break;
            }
        }
        if (sequenceId > -1) {
            this.task.updateSequenceId(sequenceId);
        }
        return payload;
    }

    /**
     * @return true: big row end.
     */
    private boolean readOneBigRow(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_BIG_ROW);
        final MySQLColumnMeta[] columnMetaArray = Objects.requireNonNull(this.rowMeta, "this.rowMeta").columnMetaArray;
        final BigRowData bigRowData = Objects.requireNonNull(this.bigRowData, "this.bigRowData");
        final boolean binaryReader = isBinaryReader();
        final byte[] nullBitMap = bigRowData.nullBitMap;
        ByteBuf payload = bigRowData.cachePayload;

        for (int i = bigRowData.index; i < columnMetaArray.length; bigRowData.index = ++i) {

            if (binaryReader && (nullBitMap[((i + 2) >> 3)] & (1 << ((i + 2) & 7))) != 0) {
                bigRowData.values[i] = null;
                continue;
            }
            if (!bigRowData.payloadEnd) {
                bigRowData.cachePayload = payload = mergePayload(cumulateBuffer, payload, bigRowData);
            }
            if (!binaryReader && Packets.getInt1AsInt(payload, payload.readerIndex()) == Packets.ENC_0) {
                payload.readByte(); // skip ENC_0
                bigRowData.values[i] = null;
                continue;
            }

            final MySQLColumnMeta columnMeta = columnMetaArray[i];
            final long columnBytes = obtainColumnBytes(columnMeta, payload);
            if (columnBytes == -1L) {
                // need more cumulate
                assertBigRowPayloadNotEnd(bigRowData);
                break;
            }

            if (columnBytes > BIG_COLUMN_BOUND) {
                // this block: big column
                long actualColumnBytes = Packets.readLenEnc(payload); // skip length encode prefix for big column
                if (columnMeta.sqlType == MySQLType.GEOMETRY) {
                    if (payload.readableBytes() < 4) {
                        // need more cumulate
                        assertBigRowPayloadNotEnd(bigRowData);
                        break;
                    }
                    payload.skipBytes(4);// skip geometry prefix
                    actualColumnBytes -= 4;
                }
                this.phase = Phase.READ_BIG_COLUMN;
                bigRowData.values[i] = createBigColumn(actualColumnBytes, payload);

                payload.release();
                bigRowData.cachePayload = Unpooled.EMPTY_BUFFER;
                break;
            }
            if (columnBytes > payload.readableBytes()) {
                // need more cumulate
                assertBigRowPayloadNotEnd(bigRowData);
                break;
            }
            bigRowData.values[i] = readColumnValue(payload, columnMeta);
        }

        final boolean bigRowEnd = bigRowData.index == columnMetaArray.length;
        if (bigRowEnd) {
            if (payload != bigRowData.cachePayload) {
                throw new IllegalStateException("cachePayload and bigRowData.cachePayload not match");
            }
            payload.release();
            if (!bigRowData.payloadEnd) {
                throw MySQLExceptions.createFatalIoException("bigRow unexpected end.");
            }
            this.readRowCount++;
            this.bigRowData = null;
            if (!this.task.isCancelled()) {
                this.task.next(MySQLResultRow0.from(bigRowData.values, rowMeta, this.adjutant));
            }
        }
        return bigRowEnd;

    }

    /**
     * @see #readOneBigRow(ByteBuf)
     */
    private void assertBigRowPayloadNotEnd(final BigRowData bigRowData) {
        if (bigRowData.payloadEnd) {
            throw new IllegalStateException("Big row payload unexpected end.");
        }
    }


    /**
     * @see #readOneBigRow(ByteBuf)
     */
    private BigColumn createBigColumn(final long totalBytes, final ByteBuf payload) {
        final Path directory = Paths.get(TEMP_DIRECTORY.toString(), LocalDate.now().toString());
        final Path file;
        try {
            if (!Files.exists(directory)) {
                Files.createDirectories(directory);
            }
            file = Files.createTempFile(directory, "b", "bc");
        } catch (Throwable e) {
            throw new BigRowIoException(
                    String.format("Create big column temp file failure,directory[%s]", directory), e);
        }

        final BigColumn bigColumn = new BigColumn(file, totalBytes);
        try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.WRITE)) {
            final int writeBytes = payload.readableBytes();
            payload.readBytes(out, writeBytes);
            bigColumn.wroteBytes += writeBytes;
        } catch (Throwable e) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ex) {
                throw new BigRowIoException(
                        String.format("Write big column temp file failure,file[%s]", file), e);
            }
            throw new BigRowIoException(
                    String.format("Write big column temp file failure,file[%s]", file), e);
        }
        return bigColumn;
    }

    /**
     * @return true:big column end,next invoke {@link #readOneBigRow(ByteBuf)} .
     * @see #read(ByteBuf, Consumer)
     * @see #readOneBigRow(ByteBuf)
     */
    private boolean readBigColumn(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_BIG_COLUMN);

        final BigRowData bigRowData = Objects.requireNonNull(this.bigRowData, "this.bigRowData");
        if (bigRowData.cachePayload != Unpooled.EMPTY_BUFFER) {
            throw new IllegalStateException("bigRowData.cachePayload not EMPTY_BUFFER");
        }

        final int columnIndex = bigRowData.index;
        final MySQLColumnMeta columnMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta")
                .columnMetaArray[columnIndex];
        final BigColumn bigColumn = obtainBigColumn(bigRowData.values[columnIndex], columnMeta, columnIndex);

        boolean bigColumnEnd = false;
        try (OutputStream out = Files.newOutputStream(bigColumn.path, StandardOpenOption.WRITE)) {
            final long totalBytes = bigColumn.totalBytes;
            long writtenBytes = bigColumn.wroteBytes;
            if (writtenBytes >= totalBytes) {
                throw new IllegalStateException(String.format("BigColumn[%s] wroteBytes[%s] > totalBytes[%s]"
                        , columnMeta.columnLabel, writtenBytes, totalBytes));
            }

            int payloadLength, sequenceId = -1, writeBytes;
            while (Packets.hasOnePacket(cumulateBuffer)) {
                if (bigRowData.payloadEnd) {
                    throw new IllegalStateException("big row unexpected end");
                }
                payloadLength = Packets.readInt3(cumulateBuffer);
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);

                writeBytes = (int) Math.min(totalBytes - writtenBytes, payloadLength);
                cumulateBuffer.readBytes(out, writeBytes);

                writtenBytes += writeBytes;


                if (writtenBytes == totalBytes) {
                    // this 'if' block handle big column end.
                    bigRowData.index++;
                    bigColumnEnd = true;

                    if (payloadLength < Packets.MAX_PAYLOAD) {
                        bigRowData.payloadEnd = true;
                    }
                    final int restPayload = payloadLength - writeBytes;
                    if (restPayload > 0) {
                        final ByteBuf temp = this.adjutant.allocator().buffer(restPayload, (1 << 30));
                        temp.writeBytes(cumulateBuffer, restPayload);
                        bigRowData.cachePayload = temp;
                    }
                    break;
                } else if (writtenBytes > totalBytes) {
                    throw new IllegalStateException(String.format("BigColumn[%s] wroteBytes[%s] > totalBytes[%s]"
                            , columnMeta.columnLabel, writtenBytes, totalBytes));
                }
                bigRowData.values[columnIndex] = convertBigColumnValue(columnMeta, bigColumn.path);
            }

            bigColumn.wroteBytes = writtenBytes;
            if (sequenceId > -1) {
                this.task.updateSequenceId(sequenceId);
            }
        } catch (IOException e) {
            try {
                Files.deleteIfExists(bigColumn.path);
            } catch (IOException ex) {
                throw new BigRowIoException(
                        String.format("Big row column[%s] read error.", columnMeta), e);
            }
            throw new BigRowIoException(
                    String.format("Big row column[%s] read error.", columnMeta), e);
        }
        return bigColumnEnd;

    }

    private BigColumn obtainBigColumn(final Object columnValue, final MySQLColumnMeta columnMeta
            , final int columnIndex) {
        final BigColumn bigColumn;
        if (columnValue instanceof BigColumn) {
            bigColumn = (BigColumn) columnValue;
            if (bigColumn.writeEnd()) {
                throw new IllegalStateException(String.format("this.phase is %s ,but have wrote end ."
                        , Phase.READ_BIG_COLUMN));
            }
        } else {
            throw new IllegalStateException(String.format("BigColumn[%s] index[%s] isn't %s instance."
                    , columnMeta.columnLabel
                    , columnIndex
                    , BigColumn.class.getName()));
        }
        return bigColumn;
    }


    /**
     * @see #readBigColumn(ByteBuf)
     */
    private Object convertBigColumnValue(final MySQLColumnMeta meta, final Path file) {
        final Object value;
        switch (meta.sqlType) {
            case BLOB:
            case GEOMETRY:
            case LONGBLOB:
                value = LongBinaries.fromTempPath(file);
                break;
            case TEXT:
            case LONGTEXT:
            case JSON:
                value = LongStrings.fromTempPath(file, this.adjutant.columnCharset(meta.columnCharset));
                break;
            default:
                throw new IllegalStateException(String.format("Unexpected sql type[%s]", meta.sqlType));
        }
        return value;
    }

    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s .", expectedPhase));
        }
    }


    enum RowStates {

        MORE_CUMULATE,
        END,
        END_ON_ERROR
    }

    interface RowReader {

        RowStates readRows(ByteBuf cumulateBuffer);
    }


    private enum Phase {
        READ_RESULT_META,
        READ_RESULT_ROW,
        READ_BIG_ROW,
        READ_BIG_COLUMN
    }


    private enum BigPacketMode {
        MORE_CUMULATE,
        BIG_ROW,
        BIG_PAYLOAD
    }


    private static final class BigRowData {

        private static final byte[] EMPTY_BIT_MAP = new byte[0];

        final byte[] nullBitMap;

        private final Object[] values;

        private ByteBuf cachePayload;

        private int index = 0;

        private boolean payloadEnd = false;

        BigRowData(final int columnCount, byte[] nullBitMap) {
            this.values = new Object[columnCount];
            this.nullBitMap = nullBitMap;
        }

    }

    static abstract class MySQLCurrentRow extends MySQLRow.JdbdCurrentRow {

        BigColumn bigColumn;


        private boolean bigRow;

        MySQLCurrentRow(MySQLRowMeta rowMeta) {
            super(rowMeta);
        }

        @Override
        public final boolean isBigRow() {
            return this.bigRow;
        }

        final void setBigColumn(BigColumn bigColumn) {
            if (this.bigColumn != null) {
                throw new IllegalStateException();
            }
            this.bigColumn = bigColumn;
            this.bigRow = true;
        }

        final void reset() {
            bigRow = false;
            this.doRest();
        }

        abstract void doRest();


    }//MySQLCurrentRow

    static final class BigColumn {

        final int columnIndex;

        final Path path;

        private final long totalBytes;

        private long wroteBytes = 0L;

        private BigColumn(int columnIndex, Path path, long totalBytes) {
            this.columnIndex = columnIndex;
            this.path = path;
            this.totalBytes = totalBytes;
        }


        private long restyBytes() {
            final long rest;
            rest = this.totalBytes - this.wroteBytes;
            if (rest < 0) {
                //no bug never here
                throw new IllegalStateException();
            }
            return rest;
        }

        private boolean writeBytes(final int length) {
            long wroteBytes = this.wroteBytes;
            wroteBytes += length;
            this.wroteBytes = wroteBytes;
            if (wroteBytes > this.totalBytes) {
                throw new IllegalArgumentException("length error");
            }
            return wroteBytes == this.totalBytes;
        }


    }// BigColumn


}
