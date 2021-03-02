package io.jdbd.mysql.protocol.client;

import io.jdbd.BigRowIoException;
import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.ResultRow;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.protocol.TerminatorPacket;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyDefinitions;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.result.ResultRowSink;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

abstract class AbstractResultSetReader implements ResultSetReader {

    static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/mysql/bigRow");

    final ClientProtocolAdjutant adjutant;

    private final ResultRowSink sink;

    private final Consumer<JdbdException> errorConsumer;

    private final Consumer<Integer> sequenceIdUpdater;

    private final Supplier<Boolean> errorJudger;

    final Properties properties;

    MySQLRowMeta rowMeta;

    int sequenceId = -1;

    private boolean resultSetEnd;

    private int serverStatus;

    private Phase phase = Phase.READ_RESULT_META;

    private BigRowData bigRowData;

    private Throwable error;

    AbstractResultSetReader(ResultSetReaderBuilder builder) {
        this.sink = Objects.requireNonNull(builder.rowSink, "builder.rowSink");
        this.adjutant = Objects.requireNonNull(builder.adjutant, "builder.adjutant");
        this.errorConsumer = Objects.requireNonNull(builder.errorConsumer, "builder.errorConsumer");

        this.sequenceIdUpdater = Objects.requireNonNull(builder.sequenceIdUpdater, "builder.sequenceIdUpdater");
        this.errorJudger = Objects.requireNonNull(builder.errorJudger, "builder.errorJudger");
        this.properties = adjutant.obtainHostInfo().getProperties();
    }

    @Override
    public final boolean read(final ByteBuf cumulateBuffer, Consumer<Object> statesConsumer)
            throws JdbdException {
        boolean resultSetEnd = this.resultSetEnd;
        if (resultSetEnd) {
            throw new IllegalStateException("ResultSet have ended.");
        }
        boolean continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_RESULT_META: {
                    if (readResultSetMeta(cumulateBuffer, statesConsumer)) {
                        this.phase = Phase.READ_RESULT_ROW;
                        continueRead = PacketUtils.hasOnePacket(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case READ_RESULT_ROW: {
                    resultSetEnd = readResultRows(cumulateBuffer, statesConsumer);
                    continueRead = !resultSetEnd && this.phase == Phase.READ_BIG_ROW;
                }
                break;
                case READ_BIG_ROW: {
                    if (readBigRow(cumulateBuffer)) {
                        this.phase = Phase.READ_RESULT_ROW;
                        continueRead = PacketUtils.hasOnePacket(cumulateBuffer);
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
                    throw MySQLExceptions.createUnknownEnumException(this.phase);
            }
        }
        if (resultSetEnd) {
            if (noError() && isResettable()) {
                resetReader();
            } else {
                this.resultSetEnd = true;
            }
        }
        return resultSetEnd;
    }




    /*################################## blow packet template method ##################################*/

    abstract boolean isResettable();

    /**
     * @return true: read result set meta end.
     * @see #read(ByteBuf, Consumer)
     */
    abstract boolean readResultSetMeta(ByteBuf cumulateBuffer, Consumer<Object> statesConsumer);

    abstract ResultRow readOneRow(ByteBuf payload);

    abstract long obtainColumnBytes(MySQLColumnMeta columnMeta, final ByteBuf bigPayloadBuffer);

    /**
     * @return maybe null ,only when {@code DATETIME} is zero.
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     */
    @Nullable
    abstract Object internalReadColumnValue(ByteBuf payload, MySQLColumnMeta columnMeta);

    abstract boolean isBinaryReader();

    abstract int skipNullColumn(BigRowData bigRowData, ByteBuf payload, int columnIndex);

    /*################################## blow final packet method ##################################*/

    /**
     * @return true: read ResultSet end.
     * @see #read(ByteBuf, Consumer)
     */
    final boolean readResultRows(final ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer) {
        assertPhase(Phase.READ_RESULT_ROW);
        final ResultRowSink sink = Objects.requireNonNull(this.sink, "this.sink");

        boolean resultSetEnd = false;
        int sequenceId = -1;
        final boolean binaryReader = isBinaryReader();
        final int negotiatedCapability = this.adjutant.obtainNegotiatedCapability();
        final boolean notCancelled = !sink.isCancelled();
        outFor:
        for (int payloadLength, readableBytes, header; ; ) {
            readableBytes = cumulateBuffer.readableBytes();
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            payloadLength = PacketUtils.getInt3(cumulateBuffer, cumulateBuffer.readerIndex()); // read payload length
            if (readableBytes < (PacketUtils.HEADER_SIZE + payloadLength)) {
                break;
            }
            final ByteBuf payload;
            if (payloadLength == PacketUtils.MAX_PAYLOAD) {
                // this 'if' block handle multi packet
                final int multiPayloadLength = PacketUtils.obtainMultiPayloadLength(cumulateBuffer);
                switch (multiPayloadLength) {
                    case -1:
                        break outFor; // more cumulate
                    case Integer.MIN_VALUE: {
                        prepareForBigRow(); // big row
                    }
                    break outFor;
                    default: {
                        payload = PacketUtils.readBigPayload(cumulateBuffer, multiPayloadLength
                                , this::updateSequenceId, this.adjutant::createByteBuffer);
                        payloadLength = payload.readableBytes();
                        sequenceId = this.sequenceId;
                    }
                }
            } else {
                cumulateBuffer.skipBytes(3); // skip payload length
                sequenceId = PacketUtils.readInt1(cumulateBuffer); // read packet sequence_id
                payload = cumulateBuffer;
            }
            header = PacketUtils.getInt1(payload, payload.readerIndex());
            if (header == ErrorPacket.ERROR_HEADER) {
                ByteBuf errorPayload = (payload == cumulateBuffer) ? cumulateBuffer.readSlice(payloadLength) : payload;
                ErrorPacket error;
                error = ErrorPacket.readPacket(errorPayload, negotiatedCapability
                        , this.adjutant.obtainCharsetResults());
                emitError(MySQLExceptions.createErrorPacketException(error));
                resultSetEnd = true;
                break;
            } else if (header == EofPacket.EOF_HEADER && (binaryReader || payloadLength < PacketUtils.MAX_PAYLOAD)) {
                ByteBuf eofPayload = (payload == cumulateBuffer) ? cumulateBuffer.readSlice(payloadLength) : payload;
                // binary row terminator
                final TerminatorPacket tp;
                if ((negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0) {
                    tp = OkPacket.read(eofPayload, negotiatedCapability);
                } else {
                    tp = EofPacket.read(eofPayload, negotiatedCapability);
                }
                this.serverStatus = tp.getStatusFags();
                serverStatesConsumer.accept(tp.getStatusFags());

                sink.accept(MySQLResultStates.from(tp));
                resultSetEnd = true;
                break;
            } else {
                final int payloadStartIndex = payload.readerIndex();
                ResultRow row = readOneRow(payload);
                if (notCancelled && noError()) {
                    //if no error,publish to downstream
                    sink.next(row);
                }
                if (payload == cumulateBuffer) {
                    cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);
                }

            }

        }

        if (sequenceId > -1) {
            updateSequenceId(sequenceId);
        }
        return resultSetEnd;
    }


    final boolean doReadRowMeta(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_RESULT_META);

        final ClientProtocolAdjutant adjutant = this.adjutant;

        MySQLRowMeta rowMeta = this.rowMeta;
        if (rowMeta == null) {
            int columnCount = PacketUtils.readLenEncAsInt(cumulateBuffer);
            rowMeta = MySQLRowMeta.from(new MySQLColumnMeta[columnCount], adjutant.obtainCustomCollationMap());
        }
        final MySQLColumnMeta[] columnMetaArray = rowMeta.columnMetaArray;

        int metaIndex = rowMeta.metaIndex;

        metaIndex = readColumnMeta(cumulateBuffer, columnMetaArray, metaIndex, this::updateSequenceId, this.adjutant);

        if (metaIndex > rowMeta.metaIndex) {
            rowMeta.metaIndex = metaIndex;
        }
        return rowMeta.isReady();
    }


    final void updateSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
        this.sequenceIdUpdater.accept(sequenceId);
    }

    /**
     * @see #readResultRows(ByteBuf, Consumer)
     */
    final void prepareForBigRow() {
        MySQLRowMeta rowMeta = Objects.requireNonNull(this.rowMeta, "this.rowMeta");
        this.bigRowData = new BigRowData(rowMeta.columnMetaArray.length, isBinaryReader());
        this.phase = Phase.READ_BIG_ROW;
    }

    final void emitError(JdbdException e) {
        if (this.error == null) {
            this.error = e;
        }
        this.errorConsumer.accept(e);
    }

    final boolean noError() {
        return this.error == null && !this.errorJudger.get();
    }


    @Nullable
    final LocalDate handleZeroDateBehavior() {
        PropertyDefinitions.ZeroDatetimeBehavior behavior;
        behavior = this.adjutant.obtainHostInfo().getProperties().getOrDefault(PropertyKey.zeroDateTimeBehavior
                , PropertyDefinitions.ZeroDatetimeBehavior.class);
        LocalDate date = null;
        switch (behavior) {
            case EXCEPTION: {
                emitError(new JdbdSQLException(new SQLException("DATETIME type can't is 0.")));
            }
            break;
            case ROUND: {
                date = LocalDate.of(1, 1, 1);
            }
            break;
            case CONVERT_TO_NULL:
                break;
            default:
                throw MySQLExceptions.createUnknownEnumException(behavior);
        }
        return date;
    }

    @Nullable
    final Object readColumnValue(final ByteBuf payload, MySQLColumnMeta columnMeta) {
        Object value;
        try {
            value = internalReadColumnValue(payload, columnMeta);
        } catch (Throwable e) {
            value = null;
            String m = String.format("Read Text ResultSet column[%s] error,", columnMeta.columnAlias);
            emitError(new JdbdSQLException(new SQLException(m, e)));
        }
        return value;
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #read(ByteBuf, Consumer)
     */
    private void resetReader() {
        final int serverStatus = this.serverStatus;
        final boolean hasMoreResults = (serverStatus & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) != 0;
        final boolean hasMoreFetch = (serverStatus & ClientProtocol.SERVER_STATUS_CURSOR_EXISTS) != 0
                && (serverStatus & ClientProtocol.SERVER_STATUS_LAST_ROW_SENT) == 0;
        if (hasMoreResults || hasMoreFetch) {

            this.phase = Phase.READ_RESULT_META;
            this.resultSetEnd = false;
            this.sequenceId = -1;
            this.serverStatus = 0;

            this.bigRowData = null;
            this.rowMeta = null;
            this.error = null;
        } else {
            this.resultSetEnd = true;
        }
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
        for (int i = bigRowData.index, payloadLength; i < columnMetaArray.length
                ; bigRowData.index = ++i) {

            ByteBuf packetPayload;
            if (rowPayloadEnd) {
                packetPayload = cachePayload;
                payloadLength = cachePayload.readableBytes();
            } else if (PacketUtils.hasOnePacket(cumulateBuffer)) {
                payloadLength = PacketUtils.readInt3(cumulateBuffer);
                sequenceId = PacketUtils.readInt1(cumulateBuffer);
                if (i == 0) {
                    // this block handle first payload of big row.
                    if (payloadLength != ClientProtocol.MAX_PACKET_SIZE) {
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
                    BigColumn bigColumn = createBigColumn(PacketUtils.readLenEnc(payloadBuffer));
                    bigRowValues[i] = bigColumn;
                    if (payloadBuffer != cachePayload) {
                        cachePayload = cumulateCachePayloadBuffer(cachePayload, packetPayload);
                    }
                    this.phase = Phase.READ_BIG_COLUMN;
                    break ourFor;
                } else {
                    throw MySQLExceptions.createFatalIoException("Server send binary column[%s] length error."
                            , columnMeta.columnAlias);
                }
                if (payloadLength < PacketUtils.MAX_PAYLOAD) {
                    break; // break while
                }
                if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
                    break ourFor;
                }
                payloadLength = PacketUtils.readInt3(cumulateBuffer);
                sequenceId = PacketUtils.readInt1(cumulateBuffer);
                packetPayload = cumulateBuffer.readSlice(payloadLength);
            }

            if (payloadLength < PacketUtils.MAX_PAYLOAD && i > 0) {
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
            if (noError()) {
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
            payloadBuffer = this.adjutant.createByteBuffer(payloadLength);
        } else if (payloadBuffer.maxFastWritableBytes() < payloadLength) {
            ByteBuf tempBuffer = this.adjutant.createByteBuffer(payloadBuffer.readableBytes() + payloadLength);
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
                    , columnMetas[bigColumnIndex].columnAlias
                    , bigColumnIndex
                    , BigColumn.class.getName()));
        }
        boolean bigColumnEnd = false;
        try (OutputStream out = Files.newOutputStream(bigColumn.path, StandardOpenOption.WRITE)) {
            final long totalBytes = bigColumn.totalBytes;
            long writtenBytes = bigColumn.wroteBytes;
            if (writtenBytes >= totalBytes) {
                throw new IllegalStateException(String.format("BigColumn[%s] wroteBytes[%s] > totalBytes[%s]"
                        , columnMetas[bigColumnIndex].columnAlias, writtenBytes, totalBytes));
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
            while (PacketUtils.hasOnePacket(cumulateBuffer)) {
                payloadLength = PacketUtils.readInt3(cumulateBuffer);
                sequenceId = PacketUtils.readInt1(cumulateBuffer);

                writeBytes = (int) Math.min(totalBytes - writtenBytes, payloadLength);
                cumulateBuffer.readBytes(out, writeBytes);

                writtenBytes += writeBytes;


                if (writtenBytes == totalBytes) {
                    // this 'if' block handle big column end.
                    bigRowData.index++;
                    bigColumnEnd = true;

                    if (payloadLength < PacketUtils.MAX_PAYLOAD) {
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
                            , columnMetas[bigColumnIndex].columnAlias, writtenBytes, totalBytes));
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
            , Consumer<Integer> sequenceIdUpdater, ClientProtocolAdjutant adjutant) {
        if (metaIndex < 0 || metaIndex >= columnMetaArray.length) {
            throw new IllegalArgumentException("metaIndex  error.");
        }
        int sequenceId = -1;
        for (int payloadStartIndex, payloadLength; metaIndex < columnMetaArray.length; metaIndex++) {
            if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
                break;
            }
            payloadLength = PacketUtils.readInt3(cumulateBuffer);//skip payload length
            sequenceId = PacketUtils.readInt1(cumulateBuffer);
            payloadStartIndex = cumulateBuffer.readerIndex();

            columnMetaArray[metaIndex] = MySQLColumnMeta.readFor41(cumulateBuffer, adjutant);
            cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);//to next packet,avoid tail filler
        }
        if (sequenceId > 0) {
            sequenceIdUpdater.accept(sequenceId);
        }
        return metaIndex;
    }


    private enum Phase {
        READ_RESULT_META,
        READ_RESULT_ROW,
        READ_BIG_ROW,
        READ_BIG_COLUMN
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
