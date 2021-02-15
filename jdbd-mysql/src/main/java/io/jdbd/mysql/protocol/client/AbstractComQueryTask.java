package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.MultiResultsSink;
import io.jdbd.vendor.TaskSignal;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class AbstractComQueryTask extends MySQLCommunicationTask {

    private static final int MEMORY_LIMIT = (int) (Runtime.getRuntime().totalMemory() * 0.2D);

    static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/mysql/bigRow");


    private final Function<MySQLCommunicationTask, ByteBuf> bufFunction;

    private final int expectedResultCount;

    private int receiveResultCount = 0;

    // non-volatile ,because all modify in netty EventLoop .
    private DecoderType decoderType;

    // non-volatile ,because all modify in netty EventLoop .
    private TextResultDecodePhase textResultDecodePhase;

    private Path bigRowPath;

    private Exception error;


    AbstractComQueryTask(MySQLTaskAdjutant taskAdjutant, Function<MySQLCommunicationTask, ByteBuf> bufFunction
            , int expectedResultCount) {
        super(taskAdjutant);
        this.bufFunction = bufFunction;
        this.expectedResultCount = expectedResultCount;
    }

    /*################################## blow CommunicationTask method ##################################*/

    @Nullable
    @Override
    public final Publisher<ByteBuf> moreSendPacket() {
        // always null
        return null;
    }


    /*################################## blow protected template method ##################################*/

    @Override
    protected final Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal) {
        return Mono.just(Objects.requireNonNull(this.bufFunction.apply(this), "this.bufFunction return value."));
    }

    @Override
    protected boolean internalDecode(final ByteBuf cumulateBuf, Consumer<Object> serverStatusConsumer) {
        if (!PacketUtils.hasOnePacket(cumulateBuf)) {
            return false;
        }
        boolean taskEnd;
        final DecoderType decoderType = this.decoderType;
        if (decoderType != null) {
            switch (decoderType) {
                case TEXT_RESULT:
                    taskEnd = decodeTextResult(cumulateBuf);
                    break;
                case LOCAL_INFILE:
                    taskEnd = decodeLocalInfileResult(cumulateBuf);
                    break;
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(decoderType);
            }
        } else {
            taskEnd = decodeOneResultSet(cumulateBuf);
        }
        if (taskEnd && this.error != null) {
            emitError(this.error);
        }
        return taskEnd;
    }


    /*################################## blow package template method ##################################*/


    abstract void emitError(Throwable e);

    abstract void emitUpdateResult(ResultStates resultStates, boolean hasMore);

    abstract boolean emitCurrentQueryRowMeta(ResultRowMeta rowMeta);

    abstract MultiResultsSink.RowSink obtainCurrentRowSink();

    abstract void emitCurrentRowTerminator(ResultStates resultStates, boolean hasMore);


    /*################################## blow private method ##################################*/


    /**
     * @return true:task end.
     */
    private boolean decodeOneResultSet(final ByteBuf cumulateBuf) {
        final ComQueryResponse response = detectComQueryResponseType(cumulateBuf, negotiatedCapability);
        boolean taskEnd;
        switch (response) {
            case ERROR: {
                int payloadLength = PacketUtils.readInt3(cumulateBuf);
                cumulateBuf.skipBytes(1); // skip sequence_id
                Charset charsetResults = this.executorAdjutant.obtainCharsetResults();
                ErrorPacket error;
                error = ErrorPacket.readPacket(cumulateBuf.readSlice(payloadLength)
                        , this.negotiatedCapability, charsetResults);
                emitError(MySQLExceptionUtils.createSQLException(error)); //emit error packet
                taskEnd = true;
            }
            break;
            case OK: {
                int payloadLength = PacketUtils.readInt3(cumulateBuf);
                updateSequenceId(PacketUtils.readInt1(cumulateBuf));
                OkPacket okPacket;
                okPacket = OkPacket.read(cumulateBuf.readSlice(payloadLength), this.negotiatedCapability);
                boolean hasMore = (okPacket.getStatusFags() & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) != 0;
                emitUpdateResult(MySQLResultStates.from(okPacket), hasMore); // emit dml sql result set.
                taskEnd = emitSuccess(hasMore);
            }
            break;
            case LOCAL_INFILE_REQUEST: {
                this.decoderType = DecoderType.LOCAL_INFILE;
                sendLocalFile(cumulateBuf);
                taskEnd = false;
            }
            break;
            case TEXT_RESULT: {
                this.decoderType = DecoderType.TEXT_RESULT;
                this.textResultDecodePhase = TextResultDecodePhase.META;
                taskEnd = decodeTextResult(cumulateBuf);
            }
            break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(response);
        }
        return taskEnd;
    }


    private boolean emitSuccess(boolean hasMore) {
        return !hasMore
                && (this.expectedResultCount < 0 || (++this.receiveResultCount) == this.expectedResultCount);
    }

    /**
     * @see #decodeOneResultSet(ByteBuf)
     */
    private void sendLocalFile(final ByteBuf cumulateBuffer) {
        int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        String filePath;
        filePath = PacketUtils.readStringEof(cumulateBuffer.readSlice(payloadLength)
                , payloadLength, this.executorAdjutant.obtainCharsetResults());
        // task executor will send.
    }

    private boolean decodeTextResult(ByteBuf cumulateBuffer) {
        boolean taskEnd = false;
        final TextResultDecodePhase phase = this.textResultDecodePhase;
        switch (phase) {
            case META: {
                MySQLColumnMeta[] columnMetas = readResultColumnMetas(cumulateBuffer);
                if (columnMetas.length == 0) {
                    break;
                }
                ResultRowMeta rowMeta = MySQLRowMeta.from(columnMetas, this.executorAdjutant.obtainCustomCollationMap());
                if (emitCurrentQueryRowMeta(rowMeta) && this.error == null) {
                    this.error = SubscriptionNotMatchException.expectBatchUpdate();
                }
                this.textResultDecodePhase = TextResultDecodePhase.ROWS;
                if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
                    break;
                }
                taskEnd = this.decodeMultiRowData(cumulateBuffer);
                if (taskEnd
                        || !PacketUtils.hasOnePacket(cumulateBuffer)
                        || this.textResultDecodePhase != TextResultDecodePhase.TERMINATOR) {
                    break;
                }
                taskEnd = decodeRowTerminator(cumulateBuffer);
            }
            break;
            case ROWS: {
                // task end ,if read error packet
                taskEnd = this.decodeMultiRowData(cumulateBuffer);
                if (taskEnd
                        || !PacketUtils.hasOnePacket(cumulateBuffer)
                        || this.textResultDecodePhase != TextResultDecodePhase.TERMINATOR) {
                    break;
                }
                taskEnd = decodeRowTerminator(cumulateBuffer);
            }
            break;
            case TERMINATOR: {
                taskEnd = decodeRowTerminator(cumulateBuffer);
            }
            break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(phase);
        }
        return taskEnd;
    }

    /**
     * @return columnMeta or empty {@link MySQLColumnMeta}
     * @see #decodeTextResult(ByteBuf)
     */
    private MySQLColumnMeta[] readResultColumnMetas(ByteBuf cumulateBuffer) {
        final int originalStartIndex = cumulateBuffer.readerIndex();

        int packetStartIndex = cumulateBuffer.readerIndex();
        int packetLength = PacketUtils.HEADER_SIZE + PacketUtils.readInt3(cumulateBuffer);

        int sequenceId = PacketUtils.readInt1(cumulateBuffer);

        final byte metadataFollows;
        final boolean hasOptionalMeta = (this.negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;
        if (hasOptionalMeta) {
            metadataFollows = cumulateBuffer.readByte();
        } else {
            metadataFollows = -1;
        }
        final int columnCount = PacketUtils.readLenEncAsInt(cumulateBuffer);

        cumulateBuffer.readerIndex(packetStartIndex + packetLength);  // to next packet

        // below column meta packet
        MySQLColumnMeta[] columnMetas = new MySQLColumnMeta[columnCount];
        int receiveColumnCount = 0;
        if (!hasOptionalMeta || metadataFollows == 1) {
            for (int i = 0, readableBytes; i < columnCount; i++) {
                readableBytes = cumulateBuffer.readableBytes();
                if (readableBytes < PacketUtils.HEADER_SIZE) {
                    break;
                }
                packetStartIndex = cumulateBuffer.readerIndex();//recode payload start index
                packetLength = PacketUtils.HEADER_SIZE + PacketUtils.readInt3(cumulateBuffer);

                if (readableBytes < packetLength) {
                    cumulateBuffer.readerIndex(packetStartIndex);
                    break;
                }
                sequenceId = PacketUtils.readInt1(cumulateBuffer);

                columnMetas[i] = MySQLColumnMeta.readFor41(cumulateBuffer, this.executorAdjutant);
                cumulateBuffer.readerIndex(packetStartIndex + packetLength); // to next packet.
                receiveColumnCount++;
            }
        }
        if ((this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            if (PacketUtils.hasOnePacket(cumulateBuffer)) {
                packetStartIndex = cumulateBuffer.readerIndex();
                packetLength = PacketUtils.HEADER_SIZE + PacketUtils.readInt3(cumulateBuffer);
                sequenceId = PacketUtils.readInt1(cumulateBuffer);

                EofPacket.read(cumulateBuffer, this.negotiatedCapability);
                cumulateBuffer.readerIndex(packetStartIndex + packetLength); // to next packet.
            } else {
                receiveColumnCount = 0; // need cumulate buffer
            }
        }

        if (receiveColumnCount == columnCount) {
            updateSequenceId(sequenceId);// update sequenceId
        } else {
            cumulateBuffer.readerIndex(originalStartIndex);  // need cumulate buffer
            columnMetas = MySQLColumnMeta.EMPTY;
        }
        return columnMetas;
    }

    private boolean decodeMultiRowData(ByteBuf cumulateBuffer) {
        final MultiResultsSink.RowSink sink = obtainCurrentRowSink();
        final boolean clientDeprecateEof = (this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0;
        boolean rowPhaseEnd = false;
        int sequenceId = obtainSequenceId();
        out:
        for (int readableBytes, payloadLength, packetStartIndex; ; ) {
            readableBytes = cumulateBuffer.readableBytes();
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            packetStartIndex = cumulateBuffer.readerIndex(); //record packet start index
            payloadLength = PacketUtils.getInt3(cumulateBuffer, packetStartIndex);
            if (readableBytes < PacketUtils.HEADER_SIZE + payloadLength) {
                break;
            }
            switch (PacketUtils.getInt1(cumulateBuffer, packetStartIndex + PacketUtils.HEADER_SIZE)) {
                case ErrorPacket.ERROR_HEADER: {
                    // error terminator
                    Charset charsetResults = this.executorAdjutant.obtainCharsetResults();
                    cumulateBuffer.skipBytes(PacketUtils.HEADER_SIZE);
                    ErrorPacket error;
                    error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                            , this.negotiatedCapability, charsetResults);
                    emitError(MySQLExceptionUtils.createSQLException(error));
                }
                return true; // occur error packet , communication task end.
                case EofPacket.EOF_HEADER: {
                    if (clientDeprecateEof && payloadLength < PacketUtils.ENC_3_MAX_VALUE) {
                        //OK terminator
                        rowPhaseEnd = true;
                        break out;
                    } else if (!clientDeprecateEof && payloadLength < 6) {
                        // EOF terminator
                        rowPhaseEnd = true;
                        break out;
                    } else {
                        throw MySQLExceptionUtils.createFatalIoException("MySQL server send error ResultSet terminator.");
                    }
                }
                default: {
                    int tempSequenceId;
                    tempSequenceId = readOneRow(cumulateBuffer, sink, payloadLength, sequenceId);
                    if (tempSequenceId < 0) {
                        // more cumulate
                        break out;
                    }
                    sequenceId = tempSequenceId;
                }

            }
        }
        if (sequenceId > -1) {
            updateSequenceId(sequenceId);
        }
        if (rowPhaseEnd) {
            this.textResultDecodePhase = TextResultDecodePhase.TERMINATOR;
        }
        return false;
    }

    /**
     * @return negative ,more cumulate.
     * @see #decodeMultiRowData(ByteBuf)
     */
    private int readOneRow(ByteBuf cumulateBuffer, MultiResultsSink.RowSink sink
            , final int payloadLength, final int lastSequenceId) {
        final MySQLRowMeta rowMeta = (MySQLRowMeta) sink.getRowMeta();
        int sequenceId = lastSequenceId;
        if (this.bigRowPath != null) {
            sequenceId = readBigRowToFile(cumulateBuffer, sink, lastSequenceId);
        } else if (payloadLength == ClientProtocol.MAX_PACKET_SIZE) {
            sequenceId = tryReadBigRow(cumulateBuffer, sink);
        } else {
            cumulateBuffer.skipBytes(3); // skip payload length
            if ((++sequenceId) == SEQUENCE_ID_MODEL) {
                sequenceId = 0;
            }
            if (PacketUtils.readInt1(cumulateBuffer) != sequenceId) {
                throw createSequenceIdError(sequenceId, cumulateBuffer);
            }
            final int payloadStarIndex = cumulateBuffer.readerIndex();
            if (!sink.isCanceled()) {
                sink.next(decodeOneRow(cumulateBuffer, rowMeta)); // emit one row
            }
            cumulateBuffer.readerIndex(payloadStarIndex + payloadLength); // to next packet,avoid filler in tail.
        }
        return sequenceId;
    }

    private int tryReadBigRow(ByteBuf cumulateBuffer, MultiResultsSink.RowSink sink) {
        int totalLength = 0;
        BigRowPhase bigRowPhase = null;

        final int originalReaderIndex = cumulateBuffer.readerIndex();
        final int bigRowBoundary = obtainBigRowUpperBoundary();
        for (int readableBytes, sequenceId = -1, payloadLength; ; ) {
            readableBytes = cumulateBuffer.readableBytes();
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            payloadLength = PacketUtils.readInt3(cumulateBuffer);
            if (readableBytes < PacketUtils.HEADER_SIZE + payloadLength) {
                break;
            }
            if ((++sequenceId) == SEQUENCE_ID_MODEL) {
                sequenceId = 0;
            }
            if (PacketUtils.readInt1(cumulateBuffer) != sequenceId) {
                throw createSequenceIdError(sequenceId, cumulateBuffer);
            }
            totalLength += payloadLength;
            if (totalLength > bigRowBoundary) {
                bigRowPhase = BigRowPhase.FILE;
                break;
            }
            if (payloadLength < ClientProtocol.MAX_PACKET_SIZE) {
                bigRowPhase = BigRowPhase.MEMORY;
                break;
            }

        }
        cumulateBuffer.readerIndex(originalReaderIndex); // reset reader index
        int sequenceId;
        if (bigRowPhase == null) {
            sequenceId = -1;
        } else {
            switch (bigRowPhase) {
                case MEMORY:
                    sequenceId = readBigRowToMemory(cumulateBuffer, sink, totalLength);
                    break;
                case FILE:
                    sequenceId = readBigRowToFile(cumulateBuffer, sink, -1);
                    break;
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(bigRowPhase);
            }
        }
        return sequenceId;
    }

    private int readBigRowToMemory(ByteBuf cumulateBuffer, MultiResultsSink.RowSink sink, int totalLength) {
        final ByteBuf payloadBuffer;
        if (sink.isCanceled()) {
            payloadBuffer = null;
        } else {
            payloadBuffer = this.executorAdjutant.createPayloadBuffer(totalLength);
        }
        int sequenceId = -1;
        for (int payloadLength; ; ) {
            payloadLength = PacketUtils.readInt3(cumulateBuffer);
            if ((++sequenceId) == SEQUENCE_ID_MODEL) {
                sequenceId = 0;
            }
            if (PacketUtils.readInt1(cumulateBuffer) != sequenceId) {
                throw createSequenceIdError(sequenceId, cumulateBuffer);
            }
            if (payloadBuffer == null) {
                cumulateBuffer.skipBytes(payloadLength);
            } else {
                cumulateBuffer.readBytes(payloadBuffer, payloadLength);
            }
            if (payloadLength < ClientProtocol.MAX_PACKET_SIZE) {
                break;
            }
        }
        if (payloadBuffer != null) {
            if (!sink.isCanceled()) {
                sink.next(decodeOneRow(cumulateBuffer, (MySQLRowMeta) sink.getRowMeta())); // emit one row
            }
            payloadBuffer.release();
        }
        return sequenceId;
    }

    private int readBigRowToFile(ByteBuf cumulateBuffer, MultiResultsSink.RowSink sink, int sequenceId) {
        final Path bigRowFile = this.bigRowPath;
        if (this.error != null) {
            return skipCurrentBigRow(cumulateBuffer, sequenceId);
        }
        final Path tempFile;
        if (bigRowFile == null) {
            try {
                tempFile = createBigRowTempFile();
                sequenceId = -1;
                this.bigRowPath = tempFile;
            } catch (IOException e) {
                this.error = new JdbdIoException("Cannot create temp file.", e);
                this.bigRowPath = TEMP_DIRECTORY;
                return skipCurrentBigRow(cumulateBuffer, -1);
            }
        } else {
            tempFile = bigRowFile;
            sequenceId = obtainSequenceId();
        }
        boolean payloadEnd = false, ioError = false;
        try (OutputStream output = Files.newOutputStream(tempFile)) {

            for (int readableBytes, payloadLength; ; ) {
                readableBytes = cumulateBuffer.readableBytes();
                if (readableBytes < PacketUtils.HEADER_SIZE) {
                    break;
                }
                payloadLength = PacketUtils.readInt3(cumulateBuffer);
                if (readableBytes < PacketUtils.HEADER_SIZE + payloadLength) {
                    cumulateBuffer.readerIndex(cumulateBuffer.readerIndex() - 3);
                    break;
                }
                if ((++sequenceId) == SEQUENCE_ID_MODEL) {
                    sequenceId = 0;
                }
                if (PacketUtils.readInt1(cumulateBuffer) != sequenceId) {
                    throw createSequenceIdError(sequenceId, cumulateBuffer);
                }
                if (ioError) {
                    cumulateBuffer.skipBytes(payloadLength);
                } else {
                    try {
                        cumulateBuffer.readBytes(output, payloadLength); //output to temp file.
                    } catch (IOException e) {
                        ioError = true;
                        this.error = new BigRowIoException("Can't write bit row content.", e, tempFile);
                    }
                }
                if (payloadLength < ClientProtocol.MAX_PACKET_SIZE) {
                    payloadEnd = true;
                    break;
                }

            }
            if (payloadEnd) {
                final Path bigRowPath = this.bigRowPath;
                this.bigRowPath = null;
                if (this.error == null) {
                    sink.next(MySQLResultRow.from(bigRowPath, (MySQLRowMeta) sink.getRowMeta(), this.executorAdjutant));
                }
            }
        } catch (IOException e) {
            // open temp file failure
            this.error = new BigRowIoException("Can't open temp file.", e, tempFile);
            sequenceId = skipCurrentBigRow(cumulateBuffer, sequenceId);
        }
        return sequenceId;
    }

    private int skipCurrentBigRow(ByteBuf cumulateBuffer, int sequenceId) {
        if (this.bigRowPath != null) {
            throw new IllegalStateException("this.bigRowPath is null.");
        }
        for (int payloadLength, readableBytes; ; ) {
            readableBytes = cumulateBuffer.readableBytes();
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            payloadLength = PacketUtils.readInt3(cumulateBuffer);
            if (readableBytes < PacketUtils.HEADER_SIZE + payloadLength) {
                cumulateBuffer.readerIndex(cumulateBuffer.readerIndex() - 3);
                break;
            }
            if ((++sequenceId) == SEQUENCE_ID_MODEL) {
                sequenceId = 0;
            }
            if (PacketUtils.readInt1(cumulateBuffer) != sequenceId) {
                throw createSequenceIdError(sequenceId, cumulateBuffer);
            }
            cumulateBuffer.skipBytes(payloadLength);
            if (payloadLength < ClientProtocol.MAX_PACKET_SIZE) {
                this.bigRowPath = null; // this big row end.
                break;
            }
        }
        return sequenceId;
    }

    private int obtainBigRowUpperBoundary() {
        int bigRowBoundary = this.executorAdjutant.obtainHostInfo().getProperties()
                .getRequiredProperty(PropertyKey.bigRowMemoryUpperBoundary, Integer.class);
        if (bigRowBoundary < ClientConstants.MIN_BIG_ROW_UPPER) {
            bigRowBoundary = ClientConstants.MIN_BIG_ROW_UPPER;
        }
        return Math.min(bigRowBoundary, MEMORY_LIMIT);
    }


    private Path createBigRowTempFile() throws IOException {
        Path tempDirectory;
        if (!Files.exists(TEMP_DIRECTORY)) {
            try {
                tempDirectory = Files.createDirectories(TEMP_DIRECTORY);
            } catch (IOException e) {
                tempDirectory = null;
            }
        } else {
            tempDirectory = TEMP_DIRECTORY;
        }
        Path tempFile;
        if (tempDirectory == null) {
            tempFile = Files.createTempFile("bigRow", "row");
        } else {
            tempFile = Files.createTempFile(tempDirectory, "bigRow", "row");

        }
        return tempFile;
    }


    private boolean decodeRowTerminator(ByteBuf cumulateBuffer) {
        if (this.decoderType != DecoderType.TEXT_RESULT
                || this.textResultDecodePhase != TextResultDecodePhase.TERMINATOR) {
            throw new IllegalStateException(String.format("decoderType[%s] and textResultDecodePhase[%s] error."
                    , this.decoderType, this.textResultDecodePhase));
        }

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));

        final TerminatorPacket terminator;
        if ((this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0) {
            // ok terminator
            terminator = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
        } else {
            // eof terminator
            terminator = EofPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
        }
        final boolean hasMore = (terminator.getStatusFags() & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) != 0;
        emitCurrentRowTerminator(MySQLResultStates.from(terminator), hasMore); // emit
        this.decoderType = null;
        this.textResultDecodePhase = null;
        return emitSuccess(hasMore);
    }

    private ResultRow decodeOneRow(ByteBuf cumulateBuffer, MySQLRowMeta rowMeta) {
        MySQLColumnMeta[] columnMetas = rowMeta.columnMetaArray;
        MySQLColumnMeta columnMeta;
        Object[] columnValueArray = new Object[columnMetas.length];
        final MySQLTaskAdjutant taskAdjutant = this.executorAdjutant;

        for (int i = 0; i < columnMetas.length; i++) {
            columnMeta = columnMetas[i];
            columnValueArray[i] = ColumnParsers.parseColumn(cumulateBuffer, columnMeta, taskAdjutant);
        }
        return MySQLResultRow.from(columnValueArray, rowMeta, taskAdjutant);
    }

    private boolean decodeLocalInfileResult(ByteBuf cumulateBuffer) {
        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));

        final int payloadIndex = cumulateBuffer.readerIndex();
        boolean taskEnd;
        final int type = PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex());
        switch (type) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.executorAdjutant.obtainCharsetResults());
                emitError(MySQLExceptionUtils.createSQLException(error));
                taskEnd = true;
            }
            break;
            case EofPacket.EOF_HEADER:
            case OkPacket.OK_HEADER: {
                OkPacket ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), negotiatedCapability);
                boolean hasMore = (ok.getStatusFags() & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) != 0;
                emitUpdateResult(MySQLResultStates.from(ok), hasMore);
                taskEnd = emitSuccess(hasMore);
            }
            break;
            default:
                throw MySQLExceptionUtils.createFatalIoException("LOCAL INFILE Data response type[%s] unknown.", type);
        }
        cumulateBuffer.readerIndex(payloadIndex + payloadLength); // to next packet
        return taskEnd;
    }

    /*################################## blow  static method ##################################*/

    /**
     * invoke this method after invoke {@link PacketUtils#hasOnePacket(ByteBuf)}.
     *
     * @see #decode(ByteBuf, Consumer)
     */
    static ComQueryResponse detectComQueryResponseType(final ByteBuf cumulateBuf, final int negotiatedCapability) {
        int readerIndex = cumulateBuf.readerIndex();
        final int payloadLength = PacketUtils.getInt3(cumulateBuf, readerIndex);
        // skip header
        readerIndex += PacketUtils.HEADER_SIZE;
        ComQueryResponse responseType;
        final boolean metadata = (negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0;

        switch (PacketUtils.getInt1(cumulateBuf, readerIndex++)) {
            case 0:
                if (metadata && PacketUtils.obtainLenEncIntByteCount(cumulateBuf, readerIndex) + 1 == payloadLength) {
                    responseType = ComQueryResponse.TEXT_RESULT;
                } else {
                    responseType = ComQueryResponse.OK;
                }
                break;
            case ErrorPacket.ERROR_HEADER:
                responseType = ComQueryResponse.ERROR;
                break;
            case PacketUtils.LOCAL_INFILE:
                responseType = ComQueryResponse.LOCAL_INFILE_REQUEST;
                break;
            default:
                responseType = ComQueryResponse.TEXT_RESULT;

        }
        return responseType;
    }

    /**
     * @return new metaIndex of metaArray.
     */
    static int tryReadColumnMetas(final ByteBuf cumulateBuffer, int metaIndex, final MySQLTaskAdjutant taskAdjutant
            , final MySQLColumnMeta[] metaArray, Consumer<Integer> sequenceIdConsumer) {
        if (metaIndex < 0 || metaIndex >= metaArray.length) {
            throw new IndexOutOfBoundsException(
                    String.format("metaIndex[%s] out [0,%s) .", metaIndex, metaArray.length));
        }

        int sequenceId = -1;
        for (int readableBytes, payloadStartIndex, payloadLength; metaIndex < metaArray.length; metaIndex++) {
            readableBytes = cumulateBuffer.readableBytes();
            if (readableBytes < PacketUtils.HEADER_SIZE) {
                break;
            }
            payloadLength = PacketUtils.getInt3(cumulateBuffer, cumulateBuffer.readerIndex());
            if (readableBytes < PacketUtils.HEADER_SIZE + payloadLength) {
                break;
            }
            cumulateBuffer.skipBytes(3);//skip payload length
            sequenceId = PacketUtils.readInt1(cumulateBuffer);
            payloadStartIndex = cumulateBuffer.readerIndex();
            metaArray[metaIndex] = MySQLColumnMeta.readFor41(cumulateBuffer, taskAdjutant);
            cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);//to next packet,avoid tail filler
        }
        if (sequenceId > -1) {
            sequenceIdConsumer.accept(sequenceId);
        }
        return metaIndex;
    }


    /*################################## blow static class ##################################*/

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response.html">Protocol::COM_QUERY Response</a>
     */
    private enum ComQueryResponse {
        OK,
        ERROR,
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }

    private enum DecoderType {
        TEXT_RESULT,
        LOCAL_INFILE
    }

    private enum TextResultDecodePhase {
        META,
        ROWS,
        TERMINATOR
    }

    private enum BigRowPhase {
        MEMORY,
        FILE
    }

}
