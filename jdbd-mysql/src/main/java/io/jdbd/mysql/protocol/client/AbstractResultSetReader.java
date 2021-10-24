package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.BigRowIoException;
import io.jdbd.result.ResultRow;
import io.jdbd.type.LongBinary;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.type.LongBinaries;
import io.jdbd.vendor.type.LongStrings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

abstract class AbstractResultSetReader implements ResultSetReader {

    static final Path TEMP_DIRECTORY = Paths.get(System.getProperty("java.io.tmpdir"), "jdbd/mysql/bigRow")
            .toAbsolutePath();

    private static final int BIG_COLUMN_BOUND = Integer.MAX_VALUE - (4 << 7);

    final TaskAdjutant adjutant;

    final StmtTask task;

    final Properties properties;

    final int negotiatedCapability;

    private MySQLRowMeta rowMeta;

    private States states = States.MORE_CUMULATE;

    private boolean resultSetEnd;

    private long readRowCount = 0;

    private Phase phase = Phase.READ_RESULT_META;

    private BigRowData bigRowData;

    private ByteBuf bigPayload;

    private long invokerCount = 0;

    AbstractResultSetReader(StmtTask task) {
        this.task = task;
        this.adjutant = task.adjutant();
        this.properties = this.adjutant.host().getProperties();
        this.negotiatedCapability = this.adjutant.capability();
    }

    @Override
    public final States read(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer)
            throws JdbdException {
        boolean resultSetEnd = this.resultSetEnd;
        if (resultSetEnd) {
            throw new IllegalStateException("ResultSet have ended.");
        }
        try {
            boolean continueRead = Packets.hasOnePacket(cumulateBuffer);
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
                        throw MySQLExceptions.createUnexpectedEnumException(this.phase);
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

    abstract ResultRow readOneRow(ByteBuf cumulateBuffer, MySQLRowMeta rowMeta);

    /**
     * @return -1 : more cumulate.
     */
    abstract long obtainColumnBytes(MySQLColumnMeta columnMeta, ByteBuf payload);

    /**
     * @return maybe null ,only when {@code DATETIME} is zero.
     * @see #readColumnValue(ByteBuf, MySQLColumnMeta)
     */
    @Nullable
    abstract Object readColumnValue(ByteBuf cumulateBuffer, MySQLColumnMeta columnMeta);

    abstract boolean isBinaryReader();

    abstract Logger getLogger();


    /*################################## blow final packet method ##################################*/

    @Nullable
    final Set<String> readSetType(final ByteBuf cumulateBuffer, final Charset columnCharset) {
        final byte[] bytes;
        bytes = Packets.readBytesLenEnc(cumulateBuffer);
        final Set<String> set;
        if (bytes == null) {
            set = null;
        } else {
            set = MySQLConvertUtils.convertToSetType(new String(bytes, columnCharset));
        }
        return set;
    }

    final LongBinary readGeometry(final ByteBuf cumulateBuffer) {
        final int length = Packets.readLenEncAsInt(cumulateBuffer);
        cumulateBuffer.skipBytes(4);// skip MySQL internal 4 bytes for integer SRID
        final byte[] bytes = new byte[length - 4];
        cumulateBuffer.readBytes(bytes);
        return LongBinaries.fromArray(bytes);
    }

    @Nullable
    final Long readBitType(final ByteBuf cumulateBuffer) {
        final byte[] bytes;
        bytes = Packets.readBytesLenEnc(cumulateBuffer);
        final Long value;
        if (bytes == null) {
            value = null;
        } else {
            long v = 0L;
            for (int i = 0, bits = (bytes.length - 1) << 3; i < bytes.length; i++, bits -= 8) {
                v |= ((bytes[i] & 0xFFL) << bits);
            }
            value = v;
        }
        return value;
    }

    @Nullable
    final Object readUnknown(final ByteBuf cumulateBuffer, final MySQLColumnMeta meta, final Charset columnCharset) {
        final byte[] bytes;
        bytes = Packets.readBytesLenEnc(cumulateBuffer);
        final Object value;
        if (bytes == null) {
            value = null;
        } else if (meta.isBinary()) {
            value = LongBinaries.fromArray(bytes);
        } else {
            value = LongStrings.fromString(new String(bytes, columnCharset));
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
        this.rowMeta = MySQLRowMeta.readForText(cumulateBuffer, this.task);
    }


    @Nullable
    final LocalDate handleZeroDateBehavior(String type) {
        final Enums.ZeroDatetimeBehavior behavior;
        behavior = this.properties.getOrDefault(MyKey.zeroDateTimeBehavior
                , Enums.ZeroDatetimeBehavior.class);
        LocalDate date = null;
        switch (behavior) {
            case EXCEPTION: {
                String message = String.format("%s type can't is 0,@see jdbc url property[%s]."
                        , type, MyKey.zeroDateTimeBehavior);
                Throwable e = new JdbdSQLException(MySQLExceptions.createTruncatedWrongValue(message, null));
                this.task.addErrorToTask(e);
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
     * @return sequenceId
     */
    private int readErrorPacket(final ByteBuf cumulateBuffer) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        final int sequenceId = Packets.readInt1AsInt(cumulateBuffer);
        final ErrorPacket error;
        error = ErrorPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability
                , this.adjutant.obtainCharsetError());
        this.task.addErrorToTask(MySQLExceptions.createErrorPacketException(error));
        this.resultSetEnd = true;
        this.states = States.END_ONE_ERROR;
        return sequenceId;
    }

    /**
     * @return sequenceId
     */
    private int readTerminatePacket(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatesConsumer
            , final boolean canceled) {
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        final int sequenceId = Packets.readInt1AsInt(cumulateBuffer);

        final int negotiatedCapability = this.negotiatedCapability;
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
                this.task.next(MySQLResultRow.from(bigRowData.values, rowMeta, this.adjutant));
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
                value = LongStrings.fromTempPath(file, this.adjutant.obtainColumnCharset(meta.columnCharset));
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
