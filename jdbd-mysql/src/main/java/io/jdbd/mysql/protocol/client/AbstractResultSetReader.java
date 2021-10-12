package io.jdbd.mysql.protocol.client;

import io.jdbd.BigRowIoException;
import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.ResultRow;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.result.ResultSetReader;
import io.jdbd.vendor.result.ResultSink;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Objects;
import java.util.function.Consumer;

abstract class AbstractResultSetReader implements ResultSetReader {

    static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/mysql/bigRow");


    final ClientProtocolAdjutant adjutant;

    final StmtTask stmtTask;

    private final ResultSink sink;

    final Properties<MyKey> properties;

    final int negotiatedCapability;

    MySQLRowMeta rowMeta;

    private boolean resultSetEnd;

    private long readRowCount = 0;

    private Phase phase = Phase.READ_RESULT_META;

    private BigRowData bigRowData;

    private Throwable error;

    private ByteBuf bigPayload;

    AbstractResultSetReader(StmtTask stmtTask, ResultSink sink) {
        this.stmtTask = stmtTask;
        this.sink = sink;
        this.adjutant = stmtTask.adjutant();
        this.properties = this.adjutant.obtainHostInfo().getProperties();
        this.negotiatedCapability = this.adjutant.negotiatedCapability();
    }

    @Override
    public final boolean read(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer)
            throws JdbdException {
        boolean resultSetEnd = this.resultSetEnd;
        if (resultSetEnd) {
            throw new IllegalStateException("ResultSet have ended.");
        }
        boolean continueRead = Packets.hasOnePacket(cumulateBuffer);
        while (continueRead) {
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
                    if (readBigRow(cumulateBuffer)) {
                        this.phase = Phase.READ_RESULT_ROW;
                        continueRead = Packets.hasOnePacket(cumulateBuffer);
                    } else {
                        continueRead = this.phase == Phase.READ_BIG_COLUMN;
                    }
                }
                break;
                case READ_BIG_COLUMN: {
                    if (readBigColumn(cumulateBuffer)) {
                        continueRead = true;
                        this.phase = Phase.READ_BIG_ROW;
                    } else {
                        continueRead = false;
                    }
                }
                break;
                default:
                    throw MySQLExceptions.createUnexpectedEnumException(this.phase);
            }
        }
        if (resultSetEnd) {
            if (isResettable()) {
                resetReader();
            } else {
                this.resultSetEnd = true;
            }
        }
        return resultSetEnd;
    }


    public final boolean isResettable() {
        return this.resettable;
    }

    /*################################## blow packet template method ##################################*/


    /**
     * @return true: read result set meta end.
     * @see #read(ByteBuf, Consumer)
     */
    abstract boolean readResultSetMeta(ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer);

    abstract ResultRow readOneRow(ByteBuf cumulateBuffer, MySQLRowMeta rowMeta);

    abstract long obtainColumnBytes(MySQLColumnMeta columnMeta, final ByteBuf bigPayloadBuffer);

    /**
     * @return maybe null ,only when {@code DATETIME} is zero.
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     */
    @Nullable
    abstract Object readColumnValue(ByteBuf payload, MySQLColumnMeta columnMeta);

    abstract boolean isBinaryReader();

    abstract int skipNullColumn(BigRowData bigRowData, ByteBuf payload, int columnIndex);

    abstract Logger getLogger();

    /*################################## blow final packet method ##################################*/

    /**
     * @return true: read ResultSet end.
     * @see #read(ByteBuf, Consumer)
     */
    final boolean readResultRows(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer) {
        assertPhase(Phase.READ_RESULT_ROW);
        final Logger LOG = getLogger();
        if (LOG.isTraceEnabled()) {
            LOG.trace("read text ResultSet rows");
        }

        final ResultSink sink = this.sink;
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
                    payloadLength = Packets.readInt3(cumulateBuffer);
                    sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                    final ErrorPacket error;
                    error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability
                            , this.adjutant.obtainCharsetError());
                    this.stmtTask.addErrorToTask(MySQLExceptions.createErrorPacketException(error));
                    resultSetEnd = true;
                    break;
                } else if (header == EofPacket.EOF_HEADER && (binaryReader || payloadLength < Packets.MAX_PAYLOAD)) {
                    // this block : read ResultSet end.
                    payloadLength = Packets.readInt3(cumulateBuffer);
                    sequenceId = Packets.readInt1AsInt(cumulateBuffer);

                    final int negotiatedCapability = this.negotiatedCapability;
                    final TerminatorPacket tp;
                    if ((negotiatedCapability & Capabilities.CLIENT_DEPRECATE_EOF) != 0) {
                        tp = OkPacket.read(cumulateBuffer.readSlice(payloadLength), negotiatedCapability);
                    } else {
                        tp = EofPacket.read(cumulateBuffer.readSlice(payloadLength), negotiatedCapability);
                    }
                    serverStatesConsumer.accept(tp);
                    if (!cancelled) {
                        sink.next(MySQLResultStates.fromQuery(rowMeta.getResultIndex(), tp, readRowCount));
                    }
                    resultSetEnd = true;
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("read  ResultSet end.");
                    }
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
                            throw MySQLExceptions.createUnexpectedEnumException(mode);
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
                ResultRow row;
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
            this.stmtTask.updateSequenceId(sequenceId);
        }
        return resultSetEnd;
    }


    final void doReadRowMeta(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_RESULT_META);
        if (this.rowMeta != null) {
            throw new IllegalStateException("this.rowMeta is non-null");
        }
        this.rowMeta = MySQLRowMeta.read(cumulateBuffer, this.stmtTask);
    }



    @Nullable
    final LocalDate handleZeroDateBehavior(String type) {
        Enums.ZeroDatetimeBehavior behavior;
        behavior = this.properties.getOrDefault(MyKey.zeroDateTimeBehavior
                , Enums.ZeroDatetimeBehavior.class);
        LocalDate date = null;
        switch (behavior) {
            case EXCEPTION: {
                String message = String.format("%s type can't is 0,@see jdbc property[%s]."
                        , type, MyKey.zeroDateTimeBehavior);
                emitError(new JdbdSQLException(MySQLExceptions.createTruncatedWrongValue(message, null)));
            }
            break;
            case ROUND: {
                date = LocalDate.of(1, 1, 1);
            }
            break;
            case CONVERT_TO_NULL:
                break;
            default:
                throw MySQLExceptions.createUnexpectedEnumException(behavior);
        }
        return date;
    }


    /*################################## blow private method ##################################*/

    /**
     * @see #read(ByteBuf, Consumer)
     */
    private void resetReader() {
        this.phase = Phase.READ_RESULT_META;
        this.resultSetEnd = false;
        this.sequenceId = -1;

        this.bigRowData = null;
        this.rowMeta = null;
        this.error = null;
    }


    /**
     * <p>
     * probably modify {@link #phase}
     * </p>
     */
    private BigPacketMode readBIgPacket(final ByteBuf cumulateBuffer) {
        int sequenceId = -1;
        final ByteBuf payload = getOrCreateBigPayload();

        BigPacketMode mode = BigPacketMode.MORE_CUMULATE;
        for (int payloadLength; Packets.hasOnePacket(cumulateBuffer); ) {

            if (payload.maxWritableBytes() < Packets.getInt3(cumulateBuffer, cumulateBuffer.readerIndex())) {
                mode = BigPacketMode.BIG_ROW;
                break;
            }
            payloadLength = Packets.readInt3(cumulateBuffer);
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);
            payload.writeBytes(cumulateBuffer, payloadLength);
            if (payloadLength < Packets.MAX_PAYLOAD) {
                mode = BigPacketMode.BIG_PAYLOAD;
                break;
            }
        }

        if (sequenceId > -1) {
            this.stmtTask.updateSequenceId(sequenceId);
        }

        if (mode == BigPacketMode.BIG_ROW) {
            MySQLRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
            BigRowData bigRowData = new BigRowData(rowMeta.columnMetaArray.length, isBinaryReader());
            bigRowData.cachePayload = payload;
            this.bigPayload = null;

            this.bigRowData = bigRowData;
            this.phase = Phase.READ_BIG_ROW;
        }
        return mode;
    }


    private ByteBuf getOrCreateBigPayload() {
        ByteBuf payload = this.bigPayload;
        if (payload == null) {
            final int capacity = Packets.MAX_PAYLOAD << 1;
            final int maxCapacity = (int) (Runtime.getRuntime().freeMemory() / 10);
            payload = this.adjutant.allocator().buffer(capacity, Math.max(capacity, maxCapacity));
            this.bigPayload = payload;
        }
        return payload;
    }


    /**
     * @return true:bigRow read end.
     * @see #read(ByteBuf, Consumer)
     */
    private boolean readBigRow(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_BIG_ROW);

        final MySQLColumnMeta[] columnMetaArray = Objects.requireNonNull(this.rowMeta, "this.rowMeta").columnMetaArray;
        final BigRowData bigRowData = Objects.requireNonNull(this.bigRowData, "this.bigRowData");
        ByteBuf cachePayload = bigRowData.cachePayload;

        int sequenceId = -1;
        final byte[] nullBitMap = bigRowData.bigRowNullBitMap;
        final Object[] bigRowValues = bigRowData.bigRowValues;
        final long bigColumnBoundary = Math.min((Runtime.getRuntime().totalMemory() / 10L), (1L << 28));
        final boolean rowPayloadEnd = bigRowData.payloadEnd;
        boolean bigRowEnd = false;
        ourFor:
        for (int i = bigRowData.index, payloadLength; i < columnMetaArray.length; bigRowData.index = ++i) {

            ByteBuf packetPayload;
            if (rowPayloadEnd) {
                packetPayload = cachePayload;
                payloadLength = cachePayload.readableBytes();
            } else if (Packets.hasOnePacket(cumulateBuffer)) {
                payloadLength = Packets.readInt3(cumulateBuffer);
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                if (i == 0) {
                    // this block handle first payload of big row.
                    if (payloadLength != ClientProtocol.MAX_PAYLOAD_SIZE) {
                        throw new IllegalStateException("Not bit row,can't invoke this method.");
                    }
                    if (cumulateBuffer.readByte() != 0) {
                        throw MySQLExceptions.createFatalIoException("Binary big row packet_header[%s] error."
                                , cumulateBuffer.getByte(cumulateBuffer.readerIndex() - 1));
                    }
                    cumulateBuffer.readBytes(nullBitMap);
                    payloadLength = payloadLength - 1 - nullBitMap.length;
                }
                packetPayload = cumulateBuffer.readSlice(payloadLength);
            } else {
                break;
            }

            //below  skip null column
            i = skipNullColumn(bigRowData, packetPayload, i);
            if (i == columnMetaArray.length && payloadLength != 0) {
                throw MySQLExceptions.createFatalIoException(
                        "Not found non-null column after index[%s]", bigRowData.index);
            }
            bigRowData.index = i;
            final MySQLColumnMeta columnMeta = columnMetaArray[i];
            if (rowPayloadEnd) {
                bigRowValues[i] = readColumnValue(cachePayload, columnMeta);
                continue;
            }
            long columnBytes;
            while (true) {
                // this 'while' block handle non-null column.
                ByteBuf payloadBuffer = cachePayload.isReadable() ? cachePayload : packetPayload;
                columnBytes = obtainColumnBytes(columnMeta, payloadBuffer);
                if (columnBytes < 0L) {
                    cachePayload = cumulateCachePayloadBuffer(cachePayload, packetPayload);
                } else if (columnBytes < bigColumnBoundary) {
                    // this 'if' block handle small/medium column.
                    if (payloadBuffer.readableBytes() < columnBytes) {
                        cachePayload = cumulateCachePayloadBuffer(cachePayload, packetPayload);
                    } else {
                        bigRowValues[i] = readColumnValue(payloadBuffer, columnMeta);
                        break;
                    }
                } else if (columnMeta.typeFlag == ProtocolConstants.TYPE_LONG_BLOB
                        || columnMeta.typeFlag == ProtocolConstants.TYPE_BLOB) {
                    // this 'if' block handle big column.
                    BigColumn bigColumn = createBigColumn(Packets.readLenEnc(payloadBuffer));
                    bigRowValues[i] = bigColumn;
                    if (payloadBuffer != cachePayload) {
                        cachePayload = cumulateCachePayloadBuffer(cachePayload, packetPayload);
                    }
                    this.phase = Phase.READ_BIG_COLUMN;
                    break ourFor;
                } else {
                    throw MySQLExceptions.createFatalIoException("Server send binary column[%s] length error."
                            , columnMeta.columnLabel);
                }
                if (payloadLength < Packets.MAX_PAYLOAD) {
                    break; // break while
                }
                if (!Packets.hasOnePacket(cumulateBuffer)) {
                    break ourFor;
                }
                payloadLength = Packets.readInt3(cumulateBuffer);
                sequenceId = Packets.readInt1AsInt(cumulateBuffer);
                packetPayload = cumulateBuffer.readSlice(payloadLength);
            }

            if (payloadLength < Packets.MAX_PAYLOAD && i > 0) {
                // big row read end.
                bigRowData.payloadEnd = true;
                bigRowEnd = true;
                break;
            }
        }

        if (cachePayload.isReadable()) {
            bigRowData.cachePayload = cachePayload;
        } else {
            cachePayload.release();
            bigRowData.cachePayload = Unpooled.EMPTY_BUFFER;
        }
        if (sequenceId > -1) {
            updateSequenceId(sequenceId);
        }
        if (bigRowEnd) {
            if (bigRowData.index != columnMetaArray.length) {
                throw new IllegalStateException(String.format(
                        "BigRow end ,but index[%s] not equals columnMetaArray.length[%s]"
                        , bigRowData.index, columnMetaArray.length));
            }
            bigRowData.payloadEnd = true;
            bigRowData.cachePayload.release();
            this.bigRowData = null;
            if (this.error == null && !this.sink.isCancelled()) {
                this.sink.next(MySQLResultRow.from(bigRowData.bigRowValues, this.rowMeta, this.adjutant));
            }
        }
        return bigRowEnd;
    }


    /**
     * @see #readBigRow(ByteBuf)
     */
    private ByteBuf cumulateCachePayloadBuffer(final ByteBuf cachePayloadBuffer, ByteBuf packetPayload) {
        ByteBuf payloadBuffer = cachePayloadBuffer;
        final int payloadLength = packetPayload.readableBytes();
        if (!payloadBuffer.isReadable()) {
            payloadBuffer.release();
            payloadBuffer = this.adjutant.allocator().buffer(payloadLength);
        } else if (payloadBuffer.maxFastWritableBytes() < payloadLength) {
            ByteBuf tempBuffer = this.adjutant.allocator().buffer(payloadBuffer.readableBytes() + payloadLength);
            tempBuffer.writeBytes(payloadBuffer);
            payloadBuffer.release();
            payloadBuffer = tempBuffer;
        }
        payloadBuffer.writeBytes(packetPayload, payloadLength);
        return payloadBuffer;
    }

    /**
     * @see #readBigRow(ByteBuf)
     */
    private BigColumn createBigColumn(final long totalBytes) {
        Path directory = Paths.get(TEMP_DIRECTORY.toString(), LocalDate.now().toString());
        try {
            if (!Files.exists(directory)) {
                Files.createDirectories(directory);
            }
            Path file = Files.createTempFile(directory, "b", "bc");
            return new BigColumn(file, totalBytes);
        } catch (IOException e) {
            throw new BigRowIoException(
                    String.format("Create big column temp file failure,directory[%s]", directory), e);
        }
    }

    /**
     * @return true:big column end,next invoke {@link #readBigRow(ByteBuf)} .
     * @see #read(ByteBuf, Consumer)
     * @see #readBigRow(ByteBuf)
     */
    private boolean readBigColumn(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_BIG_COLUMN);

        final BigRowData bigRowData = Objects.requireNonNull(this.bigRowData, "this.bigRowData");
        final MySQLColumnMeta[] columnMetas = Objects.requireNonNull(this.rowMeta, "this.rowMeta").columnMetaArray;
        final int bigColumnIndex = bigRowData.index;
        Object columnValue = bigRowData.bigRowValues[bigColumnIndex];
        final BigColumn bigColumn;
        if (columnValue instanceof BigColumn) {
            bigColumn = (BigColumn) columnValue;
            if (bigColumn.writeEnd()) {
                throw new IllegalStateException(String.format("this.phase is %s ,but have wrote end ."
                        , Phase.READ_BIG_COLUMN));
            }
        } else {
            throw new IllegalStateException(String.format("BigColumn[%s] index[%s] isn't %s instance."
                    , columnMetas[bigColumnIndex].columnLabel
                    , bigColumnIndex
                    , BigColumn.class.getName()));
        }
        boolean bigColumnEnd = false;
        try (OutputStream out = Files.newOutputStream(bigColumn.path, StandardOpenOption.WRITE)) {
            final long totalBytes = bigColumn.totalBytes;
            long writtenBytes = bigColumn.wroteBytes;
            if (writtenBytes >= totalBytes) {
                throw new IllegalStateException(String.format("BigColumn[%s] wroteBytes[%s] > totalBytes[%s]"
                        , columnMetas[bigColumnIndex].columnLabel, writtenBytes, totalBytes));
            }
            final ByteBuf payload = bigRowData.cachePayload;
            if (payload.isReadable()) {
                int writeBytes = (int) Math.min(totalBytes - writtenBytes, payload.readableBytes());
                payload.readBytes(out, writeBytes);
                writtenBytes += writeBytes;

                payload.release();
                bigRowData.cachePayload = Unpooled.EMPTY_BUFFER;

                if (writtenBytes == totalBytes) {
                    //this 'if' block handle big column end.
                    this.phase = Phase.READ_BIG_ROW;
                    bigRowData.index++;
                    bigColumn.wroteBytes = writtenBytes;
                    return true;
                }
            }

            int payloadLength, sequenceId = -1, writeBytes;
            while (Packets.hasOnePacket(cumulateBuffer)) {
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
                        bigRowData.cachePayload = cumulateCachePayloadBuffer(bigRowData.cachePayload
                                , cumulateBuffer.readSlice(restPayload));

                    }
                    break;
                } else if (writtenBytes > totalBytes) {
                    throw new IllegalStateException(String.format("BigColumn[%s] wroteBytes[%s] > totalBytes[%s]"
                            , columnMetas[bigColumnIndex].columnLabel, writtenBytes, totalBytes));
                }
                bigRowData.bigRowValues[bigColumnIndex] = bigColumn.path;
            }

            bigColumn.wroteBytes = writtenBytes;
            if (sequenceId > -1) {
                updateSequenceId(sequenceId);
            }
        } catch (IOException e) {
            throw new BigRowIoException(
                    String.format("Big row column[%s] read error.", columnMetas[bigRowData.index]), e);
        }
        return bigColumnEnd;

    }

    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s .", expectedPhase));
        }
    }

    /**
     * @see #doReadRowMeta(ByteBuf)
     */
    static int readColumnMeta(final ByteBuf cumulateBuffer, final MySQLColumnMeta[] columnMetaArray, int metaIndex
            , Consumer<Integer> sequenceIdUpdater, TaskAdjutant adjutant) {
        if (metaIndex < 0 || metaIndex >= columnMetaArray.length) {
            throw new IllegalArgumentException(String.format("metaIndex[%s]  error.", metaIndex));
        }
        final boolean traceEnabled = LOG.isTraceEnabled();
        if (traceEnabled) {
            LOG.trace("read column meta start,metaIndex[{}],readableBytes[{}]"
                    , metaIndex, cumulateBuffer.readableBytes());
        }

        int sequenceId = -1;
        for (int payloadStartIndex, payloadLength; metaIndex < columnMetaArray.length; ) {
            if (!Packets.hasOnePacket(cumulateBuffer)) {
                if (traceEnabled) {
                    LOG.trace("read column meta,no packet,more cumulate.");
                }
                break;
            }
            payloadLength = Packets.readInt3(cumulateBuffer);//skip payload length
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);
            payloadStartIndex = cumulateBuffer.readerIndex();

            columnMetaArray[metaIndex++] = MySQLColumnMeta.readFor41(cumulateBuffer, adjutant);
            cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);//to next packet,avoid tail filler
        }
        if (sequenceId > -1) {
            sequenceIdUpdater.accept(sequenceId);
        }
        if (traceEnabled) {
            LOG.trace("read column meta end,metaIndex[{}],sequenceId[{}]", metaIndex, sequenceId);
        }
        return metaIndex;
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


    static final class BigRowData {

        private static final byte[] EMPTY_BIT_MAP = new byte[0];

        final byte[] bigRowNullBitMap;

        private final Object[] bigRowValues;

        private ByteBuf cachePayload = Unpooled.EMPTY_BUFFER;

        private int index = 0;

        private boolean payloadEnd = false;

        private BigRowData(final int columnCount, boolean binaryReader) {
            this.bigRowValues = new Object[columnCount];
            if (binaryReader) {
                this.bigRowNullBitMap = new byte[(columnCount + 9) / 8];
            } else {
                this.bigRowNullBitMap = EMPTY_BIT_MAP;
            }

        }

    }

    private static final class BigColumn {

        private final Path path;

        private final long totalBytes;

        private long wroteBytes = 0L;

        private BigColumn(Path path, long totalBytes) {
            this.path = path;
            this.totalBytes = totalBytes;
        }

        private boolean writeEnd() {
            return this.wroteBytes == this.totalBytes;
        }
    }


}
