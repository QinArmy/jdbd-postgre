package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLNumberUtils;
import io.jdbd.type.Geometry;
import io.jdbd.vendor.CommunicationTask;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.*;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * <p>
 *     <ol>
 *         <li>{@link #internalStart()} send COM_STMT_PREPARE</li>
 *         <li>{@link #createExecutionPacketPublisher(List)} send COM_STMT_EXECUTE</li>
 *     </ol>
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html">Prepared Statements</a>
 */
final class ComPreparedTask extends MySQLCommunicationTask implements PreparedTask {

    static Mono<PreparedTask> prepared(MySQLTaskAdjutant taskAdjutant, String sql) {
        return Mono.create(sink -> {
            // ComPreparedTask reference is hold by MySQLCommTaskExecutor.
            new ComPreparedTask(taskAdjutant, sql, sink)
                    .submit(sink::error);
        });
    }

    private static final int BUFFER_LENGTH = 8192;

    private static final int LONG_DATA_PREFIX_SIZE = 7;

    private static final int MIN_CHUNK_SIZE = BUFFER_LENGTH;

    private static final int MAX_DATA = ClientProtocol.MAX_PACKET_SIZE - LONG_DATA_PREFIX_SIZE;


    private final String sql;

    private final MonoSink<PreparedTask> taskSink;

    private List<List<BindValue>> parameterGroupList;

    private int statementId;

    private int preparedWarningCount = 0;

    private boolean hasMeta = true;

    private Phase phase = Phase.PREPARED;

    private MySQLColumnMeta[] parameterMetas;

    private MySQLColumnMeta[] columnMetas;

    private int columnIndex = 0;

    private int batchParameterIndex = 0;

    private int batchIndex = 0;

    private Publisher<ByteBuf> packetPublisher;

    private Throwable error;

    private Object sink;

    private final int blobSendChunkSize;

    private final int maxBlobPacketSize;

    private ComPreparedTask(MySQLTaskAdjutant executorAdjutant, String sql, MonoSink<PreparedTask> taskSink) {
        super(executorAdjutant);
        this.sql = sql;
        this.taskSink = taskSink;
        this.blobSendChunkSize = obtainBlobSendChunkSize();
        this.maxBlobPacketSize = Math.min(PacketUtils.HEADER_SIZE + LONG_DATA_PREFIX_SIZE + this.blobSendChunkSize
                , PacketUtils.MAX_PACKET_CAPACITY);
    }

    @Nullable
    @Override
    public Publisher<ByteBuf> moreSendPacket() {
        return null;
    }

    @Override
    public MySQLColumnMeta[] obtainParameterMeta() throws JdbdMySQLException {
        MySQLColumnMeta[] parameterMetaArray = this.parameterMetas;
        if (parameterMetaArray == null) {
            throw new JdbdMySQLException("%s[%s] not prepared yet.", CommunicationTask.class.getName(), this);
        }
        return parameterMetaArray;
    }

    @Override
    public int obtainPreparedWarningCount() throws IllegalStateException {
        if (this.phase == Phase.PREPARED) {
            throw new IllegalStateException("Not prepared yet.");
        }
        return this.preparedWarningCount;
    }

    @Override
    public MySQLColumnMeta[] obtainColumnMeta() {
        MySQLColumnMeta[] columnMetaArray = this.columnMetas;
        if (columnMetaArray == null) {
            throw new IllegalStateException(
                    String.format("%s[%s] not prepared yet.", CommunicationTask.class.getName(), this));
        }
        return columnMetaArray;
    }

    @Override
    public <T> Flux<T> executeQuery(List<BindValue> parameterGroup, BiFunction<ResultRow, ResultRowMeta, T> decoder
            , Consumer<ResultStates> statesConsumer) {
        return Flux.just();
    }

    @Override
    public Mono<ResultStates> executeUpdate(List<BindValue> parameterGroup) {
        return Mono.empty();
    }

    @Override
    public Flux<ResultStates> executeBatchUpdate(List<List<BindValue>> parameterGroupList) {
        return Flux.empty();
    }

    @Override
    public Mono<Void> close() {
        return Mono.empty();
    }

    /*################################## blow protected method ##################################*/

    @Override
    protected Publisher<ByteBuf> internalStart() {
        // this method send COM_STMT_PREPARE packet.
        // @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
        int payloadLength = (this.sql.length() * this.executorAdjutant.obtainMaxBytesPerCharClient()) + 1;
        ByteBuf packetBuffer = this.executorAdjutant.createPacketBuffer(payloadLength);

        packetBuffer.writeByte(PacketUtils.COM_STMT_PREPARE);
        packetBuffer.writeCharSequence(this.sql, this.executorAdjutant.obtainCharsetClient());
        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());

        this.phase = Phase.PREPARE_RESPONSE;
        return Mono.just(packetBuffer);
    }

    @Override
    protected boolean internalDecode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        final Phase phase = Objects.requireNonNull(this.phase, "this.phase");
        boolean taskEnd = false;
        switch (phase) {
            case PREPARE_RESPONSE: {
                taskEnd = readPrepareResponse(cumulateBuffer, serverStatusConsumer);
                if (!taskEnd && this.phase == Phase.EXECUTE) {
                    taskEnd = executeStatement();
                }
            }
            break;
            case READ_PARAM_META: {
                if (!readParameterMeta(cumulateBuffer, serverStatusConsumer)) {
                    break;
                }
                if (this.columnMetas.length == 0) {
                    this.phase = Phase.EXECUTE;
                    break;
                }
                this.phase = Phase.READ_COLUMN_META;
                if (readColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                    this.phase = Phase.EXECUTE;
                }
            }
            break;
            case READ_COLUMN_META: {
                if (readColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                    this.phase = Phase.EXECUTE;
                }
            }
            break;
            case RESET: {

            }
            break;
            case EXECUTE: {
                taskEnd = executeStatement();
            }
            break;
            case READ_RESULT_SET:
                taskEnd = readResultSet(cumulateBuffer);
                break;
            case PREPARED:
                throw new IllegalStateException("this.phase is PREPARED ,task not start yet.");
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(phase);
        }
        if (taskEnd && this.error != null) {
            emitError(this.error);
            closeStatement();
        }
        return taskEnd;
    }

    /*################################## blow private method ##################################*/

    /**
     * @return true: task end ,prepare statement failure.
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readPrepareResponse(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        final int payloadStartIndex = cumulateBuffer.readerIndex();

        final int headFlag = PacketUtils.readInt1(cumulateBuffer); //1. status/error header
        boolean taskEnd;
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.executorAdjutant.obtainCharsetResults());
                this.taskSink.error(MySQLExceptionUtils.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case 0: {
                this.statementId = PacketUtils.readInt4(cumulateBuffer);//2. statement_id
                final int numColumns = PacketUtils.readInt2(cumulateBuffer);//3. num_columns
                final int numParams = PacketUtils.readInt2(cumulateBuffer);//4. num_params
                cumulateBuffer.skipBytes(1); //5. skip filler
                this.preparedWarningCount = PacketUtils.readInt2(cumulateBuffer);//6. warning_count
                if ((this.negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
                    this.hasMeta = cumulateBuffer.readByte() != 0; //7. metadata_follows
                }
                cumulateBuffer.readerIndex(payloadStartIndex + payloadLength); // to next packet,avoid tail filler.
                readPreparedMeta(cumulateBuffer, numColumns, numParams, serverStatusConsumer);
                taskEnd = false;
            }
            break;
            default: {
                RuntimeException e = MySQLExceptionUtils.createFatalIoException(
                        "COM_STMT_PREPARE Response error. headFlag[%s]", headFlag);
                this.taskSink.error(e);
                throw e;
            }
        }

        return taskEnd;
    }

    /**
     * <p>
     * modify :
     *     <ul>
     *         <li>{@link #phase}</li>
     *          <li>{@link #parameterMetas}</li>
     *           <li>{@link #columnMetas}</li>
     *     </ul>
     * </p>
     *
     * @see #readPrepareResponse(ByteBuf, Consumer)
     */
    private void readPreparedMeta(ByteBuf cumulateBuffer, final int numColumns, final int numParams
            , Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.PREPARE_RESPONSE) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.PREPARE_RESPONSE));
        }
        // below read parameter meta and column meta.
        final boolean paramMetaReadEnd;
        if (numParams > 0) {
            this.parameterMetas = new MySQLColumnMeta[numParams];
            this.phase = Phase.READ_PARAM_META;
            paramMetaReadEnd = readParameterMeta(cumulateBuffer, serverStatusConsumer);
        } else {
            paramMetaReadEnd = true;
            this.parameterMetas = MySQLColumnMeta.EMPTY;
        }

        if (numColumns > 0) {
            this.columnMetas = new MySQLColumnMeta[numColumns];
        } else {
            this.columnMetas = MySQLColumnMeta.EMPTY;
        }

        if (numColumns > 0 && paramMetaReadEnd) {
            this.phase = Phase.READ_COLUMN_META;
            if (readColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                this.phase = Phase.EXECUTE;
            }
        } else if (paramMetaReadEnd) {
            this.phase = Phase.EXECUTE;
        }
    }

    /**
     * @return true:read end
     * @see #readPreparedMeta(ByteBuf, int, int, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readParameterMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_PARAM_META) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_PARAM_META));
        }
        int index = this.batchParameterIndex;
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
        if (index < metaArray.length) {
            index = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer, this.executorAdjutant
                    , metaArray, index, this::updateSequenceId);
            this.batchParameterIndex = index;
        }
        return index == metaArray.length && tryReadEof(cumulateBuffer, serverStatusConsumer);
    }

    /**
     * @return true:read end
     * @see #readPreparedMeta(ByteBuf, int, int, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readColumnMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_COLUMN_META) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_COLUMN_META));
        }
        int index = this.columnIndex;
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.columnMetas, "this.columnIndex");
        if (index < metaArray.length) {
            index = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer, this.executorAdjutant
                    , metaArray, index, this::updateSequenceId);
            this.columnIndex = index;
        }
        return index == metaArray.length && tryReadEof(cumulateBuffer, serverStatusConsumer);
    }

    /**
     * @return false : need more cumulate
     * @see #readParameterMeta(ByteBuf, Consumer)
     * @see #readColumnMeta(ByteBuf, Consumer)
     */
    private boolean tryReadEof(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        boolean end = true;
        if ((this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            if (PacketUtils.hasOnePacket(cumulateBuffer)) {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
                EofPacket eof;
                eof = EofPacket.readPacket(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(eof.getStatusFags());
            } else {
                end = false;
            }
        }
        return end;
    }

    /**
     * @return true:task end
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">COM_STMT_EXECUTE</a>
     */
    private boolean executeStatement() {
        if (this.phase != Phase.EXECUTE) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't %s", this.phase, Phase.EXECUTE));
        }
        final int batchIndex = this.batchIndex;
        final List<List<BindValue>> parameterGroupList = Objects.requireNonNull(
                this.parameterGroupList, "this.parameterGroupList");
        if (batchIndex == parameterGroupList.size()) {
            // no more param group,task end
            return true;
        }
        this.batchIndex++;
        final List<BindValue> parameterGroup = parameterGroupList.get(batchIndex);
        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");

        final int parameterCount = parameterGroup.size();
        if (parameterCount != parameterMetaArray.length) {
            this.error = new SQLBindParameterException(String.format(
                    "Bind parameter count[%s] not equals %s .", parameterGroup.size(), parameterMetaArray.length));
            return true;
        }
        updateSequenceId(-1); // reset sequence_id for COM_STMT_EXECUTE protocol
        boolean taskEnd;
        if (parameterCount == 0) {
            this.packetPublisher = Mono.just(createExecutePacketBuffer(10));
            taskEnd = false;
        } else {
            taskEnd = createExecutePacketPublisherWithParameters(parameterGroup);
        }
        return taskEnd;
    }

    /**
     * <p>
     * modify {@link #packetPublisher} for {@link #moreSendPacket()}
     * </p>
     *
     * @return true : task end,because bind parameter error.
     * @see #executeStatement()
     */
    private boolean createExecutePacketPublisherWithParameters(final List<BindValue> parameterGroup) {
        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");

        final int parameterCount = parameterGroup.size();

        //1. below check bind parameter and collect long data parameter.
        List<Pair<Integer, Object>> longDataList = null;
        SQLBindParameterException parameterError;
        for (int i = 0; i < parameterCount; i++) {
            BindValue bindValue = parameterGroup.get(i);
            Object value = bindValue.getValue();
            if (value == null) {
                continue;
            }
            // check bind parameter type and value match.
            parameterError = checkBindParameterType(parameterMetaArray[i], bindValue.getType(), value);
            if (parameterError != null) {
                this.error = parameterError;
                // task end
                return true;
            }
            if (bindValue.isLongData()) {
                if (longDataList == null) {
                    longDataList = new ArrayList<>();
                }
                longDataList.add(new Pair<>(i, value));
            }
        }
        //2. create packet publisher
        final Publisher<ByteBuf> packetPublisher;
        if (longDataList == null) {
            packetPublisher = createExecutionPacketPublisher(parameterGroup);
        } else {
            packetPublisher = Flux.fromIterable(longDataList)
                    .flatMap(this::sendLongData)
                    .concatWith(Mono.defer(() -> createExecutionPacketPublisher(parameterGroup)));
        }
        this.packetPublisher = packetPublisher;
        return false;
    }


    /**
     * @see #createExecutionPacketPublisher(List)
     */
    private ByteBuf createExecutePacketBuffer(int initialPayloadCapacity) {
        if (this.phase != Phase.EXECUTE) {
            throw new IllegalStateException(String.format("this.phase isn't %s", Phase.EXECUTE));
        }
        ByteBuf packetBuffer = this.executorAdjutant.createPacketBuffer(initialPayloadCapacity);

        packetBuffer.writeByte(PacketUtils.COM_STMT_EXECUTE); // 1.status
        PacketUtils.writeInt4(packetBuffer, this.statementId);// 2. statement_id
        packetBuffer.writeByte(0); //3.cursor Flags, reactive api not support cursor
        PacketUtils.writeInt4(packetBuffer, 1);//4. iteration_count,Number of times to execute the statement. Currently always 1.

        return packetBuffer;
    }

    /**
     * @return parameter value byte length ,if return {@link Integer#MIN_VALUE} ,then parameter error,should end task.
     * @throws IllegalArgumentException when {@link BindValue#getValue()} is null.
     * @see #createExecutePacketPublisherWithParameters(List)
     */
    private int obtainParameterValueLength(MySQLColumnMeta parameterMeta, final int parameterIndex, BindValue bindValue) {
        final Object value = bindValue.getValue();
        if (value == null) {
            throw new IllegalArgumentException("bindValue.getValue() is null ");
        }
        int length;
        switch (bindValue.getType()) {
            case INT:
            case INT_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case DATE:
                length = 4;
                break;
            case BIGINT:
            case BIGINT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case BIT:
                length = 8;
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                length = 1;
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                length = 2;
                break;
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                length = 3;
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                length = (int) parameterMeta.length;
                break;
            case VARCHAR:
            case CHAR:
            case SET:
            case JSON:
            case ENUM:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT: {
                if (value instanceof String) {
                    length = ((String) value).length() * obtainMLen(parameterMeta.collationIndex);
                } else if (value instanceof byte[]) {
                    length = ((byte[]) value).length;
                } else if (value instanceof ByteArrayInputStream) {
                    length = ((ByteArrayInputStream) value).available();
                } else {
                    length = emitBindParameterTypeError(value.getClass(), parameterIndex);
                }
            }
            break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB: {
                if (value instanceof byte[]) {
                    length = ((byte[]) value).length;
                } else if (value instanceof ByteArrayInputStream) {
                    length = ((ByteArrayInputStream) value).available();
                } else {
                    length = emitBindParameterTypeError(value.getClass(), parameterIndex);
                }
            }
            break;
            case DATETIME:
            case TIMESTAMP:
                length = (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) ? 11 : 7;
                break;
            case TIME:
                length = (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) ? 12 : 8;
                break;
            case NULL:
            case UNKNOWN:
            case GEOMETRY:
                length = 0; // TODO 优化
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(parameterMeta.mysqlType);
        }

        return length;
    }


    /**
     * @see #createExecutePacketPublisherWithParameters(List)
     */
    private Flux<ByteBuf> sendLongData(Pair<Integer, Object> pair) {
        final Object value = Objects.requireNonNull(pair.getSecond(), "pair.getSecond()");
        Flux<ByteBuf> flux;
        if (value instanceof byte[]) {
            flux = sendByteArrayLongData(pair.getFirst(), (byte[]) value);
        } else if (value instanceof InputStream) {
            flux = sendInputStreamLongData(pair.getFirst(), (InputStream) value);
        } else if (value instanceof ReadableByteChannel) {
            flux = sendInputReadByteChannelLongData(pair.getFirst(), (ReadableByteChannel) value);
        } else if (value instanceof Reader) {
            flux = sendReaderLongData(pair.getFirst(), (Reader) value);
        } else if (value instanceof Path) {
            flux = sendPathLongData(pair.getFirst(), (Path) value);
        } else if (value instanceof Publisher) {
            flux = sendPublisherLongData(pair.getFirst(), (Publisher<?>) value);
        } else {
            flux = Flux.error(new BindParameterException
                    (String.format("Bind parameter[%s] type[%s] not support."
                            , pair.getFirst(), pair.getSecond().getClass().getName()), pair.getFirst()));
        }
        return flux;
    }


    /**
     * @see #sendLongData(Pair)
     */
    private Flux<ByteBuf> sendByteArrayLongData(final int paramId, byte[] longData) {
        final ByteBuf packetBuffer;
        if (longData.length < MAX_DATA) {
            packetBuffer = createLongDataPacket(paramId, longData.length);
            packetBuffer.writeBytes(longData);
            PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());
        } else {
            final int totalPayload = longData.length + 7;
            final int time = (totalPayload / ClientProtocol.MAX_PACKET_SIZE) + 1;
            final int capacity = ((totalPayload / ClientProtocol.MAX_PACKET_SIZE) * PacketUtils.MAX_PACKET_CAPACITY)
                    + PacketUtils.HEADER_SIZE
                    + (totalPayload % ClientProtocol.MAX_PACKET_SIZE);

            packetBuffer = this.executorAdjutant.createPayloadBuffer(capacity);

            for (int i = 1, offset = 0, payloadLength = MAX_DATA; i <= time; i++) {
                //1. below write payload length
                if (i < time) {
                    PacketUtils.writeInt3(packetBuffer, ClientProtocol.MAX_PACKET_SIZE);
                } else {
                    PacketUtils.writeInt3(packetBuffer, payloadLength);
                }
                PacketUtils.writeInt1(packetBuffer, addAndGetSequenceId()); //2. write sequence_id;
                if (i == 1) {
                    packetBuffer.writeByte(PacketUtils.COM_STMT_SEND_LONG_DATA); //status
                    PacketUtils.writeInt4(packetBuffer, this.statementId); //statement_id
                    PacketUtils.writeInt2(packetBuffer, paramId);//param_id
                }
                if (offset < longData.length) {
                    packetBuffer.writeBytes(longData, offset, payloadLength);
                }
                offset += payloadLength;
                payloadLength = longData.length - offset;
            }
        }
        return Flux.just(packetBuffer);
    }

    /**
     * @see #sendLongData(Pair)
     */
    private Flux<ByteBuf> sendInputStreamLongData(final int parameterIndex, InputStream input) {
        return Flux.create(sink -> writeInputStreamParameter(parameterIndex, input, sink));
    }

    /**
     * @see #sendLongData(Pair)
     */
    private Flux<ByteBuf> sendInputReadByteChannelLongData(final int parameterIndex, ReadableByteChannel channel) {
        return Flux.create(sink -> {
            ByteBuf packetBuffer = null;
            try {
                packetBuffer = createLongDataPacket(parameterIndex, BUFFER_LENGTH);
                final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_LENGTH);
                while (channel.read(buffer) > 0) {
                    packetBuffer = writeByteBufferToBlobPacket(buffer, parameterIndex, packetBuffer, sink);
                }
                if (hasBlobData(packetBuffer)) {
                    PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId()); // write header
                    sink.next(packetBuffer); // send packet
                } else {
                    packetBuffer.release();
                }

            } catch (IOException e) {
                packetBuffer.release();
                sink.error(createLongDataReadException(e, channel.getClass(), parameterIndex));
            }
        });
    }

    /**
     * @see #sendLongData(Pair)
     */
    private Flux<ByteBuf> sendReaderLongData(final int parameterIndex, final Reader reader) {
        return Flux.create(sink -> {
            ByteBuf packetBuffer = null;
            try {
                final Charset clobCharset = obtainClobCharset();
                final CharBuffer charBuffer = CharBuffer.allocate(BUFFER_LENGTH >> 1);

                packetBuffer = createLongDataPacket(parameterIndex, BUFFER_LENGTH);

                ByteBuffer byteBuffer;
                while (reader.read(charBuffer) > 0) { //1. read char stream
                    byteBuffer = clobCharset.encode(charBuffer);   //2. encode char to byte
                    packetBuffer = writeByteBufferToBlobPacket(byteBuffer, parameterIndex, packetBuffer, sink); // 3.write byte to packet.
                    charBuffer.clear(); // 4. clear charBuffer
                }
                if (hasBlobData(packetBuffer)) {
                    PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId()); // write header
                    sink.next(packetBuffer); // send packet
                } else {
                    packetBuffer.release();
                }
            } catch (IOException e) {
                packetBuffer.release();
                sink.error(createLongDataReadException(e, reader.getClass(), parameterIndex));
            }
        });
    }

    /**
     * @see #sendLongData(Pair)
     */
    private Flux<ByteBuf> sendPathLongData(int parameterIndex, Path path) {
        return Flux.create(sink -> {
            try (InputStream input = Files.newInputStream(path, StandardOpenOption.READ)) {
                writeInputStreamParameter(parameterIndex, input, sink);
            } catch (IOException e) {
                sink.error(createLongDataReadException(e, path.getClass(), parameterIndex));
            }


        });
    }


    /**
     * @see #sendLongData(Pair)
     */
    private Flux<ByteBuf> sendPublisherLongData(int parameterIndex, Publisher<?> inputPublisher) {
        return Flux.create(sink -> Flux.from(inputPublisher)
                .subscribeWith(new PublisherLongDataSubscriber(sink, parameterIndex)));
    }


    private boolean hasBlobData(ByteBuf packetBuffer) {
        return packetBuffer.readableBytes() > (PacketUtils.HEADER_SIZE + LONG_DATA_PREFIX_SIZE);
    }


    private ByteBuf addBlobPacketCapacity(final ByteBuf packetBuffer, final int addCapacity) {
        final int oldCapacity = packetBuffer.capacity();
        final int capacity = Math.min(this.maxBlobPacketSize, Math.max(oldCapacity + addCapacity, oldCapacity << 1));
        ByteBuf buffer = packetBuffer;
        if (oldCapacity < capacity) {
            if (packetBuffer.maxCapacity() < capacity) {
                ByteBuf tempBuf = this.executorAdjutant.createPayloadBuffer(capacity);
                tempBuf.writeBytes(packetBuffer);
                packetBuffer.release();
                buffer = tempBuf;
            } else {
                buffer = packetBuffer.capacity(capacity);
            }
        }
        return buffer;
    }

    private BindParameterException createLongDataReadException(IOException e, Class<?> parameterClass, int parameterIndex) {
        return new BindParameterException(
                String.format("Bind parameter[%s](%s) read error.", parameterIndex, parameterClass.getName())
                , e, parameterIndex);
    }


    private int obtainBlobSendChunkSize() {
        int packetChunkSize = this.properties.getOrDefault(PropertyKey.blobSendChunkSize, Integer.class);
        if (packetChunkSize < MIN_CHUNK_SIZE) {
            packetChunkSize = MIN_CHUNK_SIZE;
        }
        return packetChunkSize;
    }

    private int obtainMaxBytesChar(Charset charset) {
        int maxBytesChar;
        if (!charset.equals(StandardCharsets.UTF_16)) {
            maxBytesChar = (int) Math.ceil(charset.newEncoder().maxBytesPerChar());
            if (maxBytesChar == 1) {
                maxBytesChar = 2; // for safety
            }
        } else {
            maxBytesChar = 4;
        }
        return maxBytesChar;
    }


    private ByteBuf createLongDataPacket(final int parameterIndex, final int chunkSize) {
        int payloadCapacity;
        if (chunkSize < 1024) {
            payloadCapacity = 1024;
        } else {
            payloadCapacity = LONG_DATA_PREFIX_SIZE + Math.min(this.blobSendChunkSize, chunkSize);
        }
        ByteBuf packetBuffer = this.executorAdjutant.createPacketBuffer(payloadCapacity);

        packetBuffer.writeByte(PacketUtils.COM_STMT_SEND_LONG_DATA); //status
        PacketUtils.writeInt4(packetBuffer, this.statementId); //statement_id
        PacketUtils.writeInt2(packetBuffer, parameterIndex);//param_id
        return packetBuffer;

    }


    private Charset obtainClobCharset() {
        Charset charset = this.properties.getProperty(PropertyKey.clobCharacterEncoding, Charset.class);
        if (charset == null) {
            charset = this.executorAdjutant.obtainCharsetClient();
        }
        return charset;
    }


    /**
     * @see #createExecutePacketPublisherWithParameters(List)
     */
    private Mono<ByteBuf> createExecutionPacketPublisher(final List<BindValue> parameterGroup)
            throws BindParameterException {
        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
        final int parameterCount = parameterGroup.size();
        final byte[] nullBitsMap = new byte[(parameterCount + 7) / 8];

        //1. make nullBitsMap and parameterValueLength
        int parameterValueLength = 0;
        for (int i = 0, length; i < parameterCount; i++) {
            BindValue bindValue = parameterGroup.get(i);
            if (bindValue.getValue() == null) {
                nullBitsMap[i / 8] |= (1 << (i & 7));
            } else if (!bindValue.isLongData()) {
                length = obtainParameterValueLength(parameterMetaArray[i], i, bindValue);
                if (length == Integer.MIN_VALUE) {
                    throw new BindParameterException(String.format("Parameter[%s] type not compatibility", i), i);
                }
                parameterValueLength += length;
            }
        }

        final ByteBuf packetBuffer = createExecutePacketBuffer(
                10 + nullBitsMap.length + 1 + (parameterCount * 2) + parameterValueLength);

        packetBuffer.writeBytes(nullBitsMap); // null_bitmap
        packetBuffer.writeByte(1); //new_params_bind_flag

        for (BindValue value : parameterGroup) {
            PacketUtils.writeInt2(packetBuffer, value.getType().parameterType); // parameter_types
        }
        for (int i = 0; i < parameterCount; i++) {
            BindValue bindValue = parameterGroup.get(i);
            if (bindValue.isLongData() || bindValue.getValue() == null) {
                continue;
            }
            String errorMsg = bindParameter(packetBuffer, parameterMetaArray[i]
                    , bindValue, this.executorAdjutant.obtainCharsetClient());
            if (errorMsg != null) { //parameter_values
                // bind parameter error , end task.
                packetBuffer.release();
                throw new BindParameterException(String.format("Parameter[%s] %s", errorMsg, i), i);
            }
        }
        return Mono.just(packetBuffer);
    }

    private void closeStatement() {

    }

    @Nullable
    private SQLBindParameterException checkBindParameterType(MySQLColumnMeta parameterMeta, MySQLType bindType
            , Object nonNullValue) {
        return null;
    }


    private boolean readResultSet(ByteBuf cumulateBuffer) {
        return false;
    }

    private void emitError(Throwable e) {
        Object sink = this.sink;
        if (sink == null) {
            throw new NullPointerException("this.sink is null");
        } else if (sink instanceof MonoSink) {
            ((MonoSink<?>) sink).error(e);
        } else if (sink instanceof FluxSink) {
            ((FluxSink<?>) sink).error(e);
        } else {
            throw new IllegalStateException(String.format("this.sink type[%s] unknown.", sink.getClass().getName()));
        }
    }


    private ByteBuf writeByteArrayToBlobPacket(final byte[] byteArray, final int length
            , final int parameterIndex, ByteBuf packetBuffer, final FluxSink<ByteBuf> sink) {
        if (length == 0) {
            return packetBuffer;
        }
        if (length <= packetBuffer.writableBytes()) {
            packetBuffer.writeBytes(byteArray);
            return packetBuffer;
        }
        // below Adjusts the capacity of this buffer
        for (int offset = 0, writableBytes, dataLen; offset < length; ) {
            dataLen = length - offset;
            packetBuffer = addBlobPacketCapacity(packetBuffer, dataLen);
            writableBytes = packetBuffer.writableBytes();

            if (dataLen > writableBytes) {
                if (writableBytes > 0) {
                    packetBuffer.writeBytes(byteArray, offset, writableBytes);
                    offset += writableBytes;
                }
                PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId()); // write header
                sink.next(packetBuffer); // send packet

                dataLen = length - offset;
                packetBuffer = createLongDataPacket(parameterIndex, dataLen); // create new packet
                dataLen = Math.min(dataLen, packetBuffer.writableBytes());
            }
            packetBuffer.writeBytes(byteArray, offset, dataLen);
            offset += dataLen;
        }
        return packetBuffer;
    }

    private ByteBuf writeByteBufferToBlobPacket(final ByteBuffer byteBuffer, final int parameterIndex
            , ByteBuf packetBuffer, final FluxSink<ByteBuf> sink) {
        if (!byteBuffer.hasRemaining()) {
            return packetBuffer;
        }

        if (byteBuffer.remaining() <= packetBuffer.writableBytes()) {
            packetBuffer.writeBytes(byteBuffer);
            return packetBuffer;
        }
        // below Adjusts the capacity of this buffer

        for (int writableBytes; byteBuffer.hasRemaining(); ) {
            packetBuffer = addBlobPacketCapacity(packetBuffer, byteBuffer.remaining());
            writableBytes = packetBuffer.writableBytes();

            if (byteBuffer.remaining() > writableBytes) {
                if (writableBytes > 0) {
                    byte[] bufferArray = new byte[writableBytes];
                    byteBuffer.get(bufferArray);
                    packetBuffer.writeBytes(bufferArray);
                }
                PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId()); // write header
                sink.next(packetBuffer); // send packet

                packetBuffer = createLongDataPacket(parameterIndex, byteBuffer.remaining()); // create new packet
            } else {
                packetBuffer.writeBytes(byteBuffer);
            }

        }
        return packetBuffer;

    }

    private void writeInputStreamParameter(final int parameterIndex, InputStream input, FluxSink<ByteBuf> sink) {
        ByteBuf packetBuffer = null;
        try {
            packetBuffer = createLongDataPacket(parameterIndex, BUFFER_LENGTH);
            final byte[] buffer = new byte[BUFFER_LENGTH];
            for (int length; (length = input.read(buffer)) > 0; ) {
                packetBuffer = writeByteArrayToBlobPacket(buffer, length, parameterIndex, packetBuffer, sink);
            }
            if (hasBlobData(packetBuffer)) {
                PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId()); // write header
                sink.next(packetBuffer); // send packet
            } else {
                packetBuffer.release();
            }

        } catch (IOException e) {
            packetBuffer.release();
            sink.error(createLongDataReadException(e, input.getClass(), parameterIndex));
        }
    }

    /**
     * @return true : task end because bind parameter occur error.
     * @throws IllegalArgumentException when {@link BindValue#getValue()} is null.
     */
    @Nullable
    private String bindParameter(ByteBuf packetBuffer, MySQLColumnMeta parameterMeta, BindValue bindValue
            , Charset clientCharset) {
        final String errorMsg;
        switch (parameterMeta.mysqlType) {
            case INT:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                errorMsg = bindToInt(packetBuffer, bindValue);
                break;
            case INT_UNSIGNED:
            case BIGINT:
            case BIGINT_UNSIGNED:
                errorMsg = bindToBigInt(packetBuffer, bindValue);
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:
                errorMsg = bindToFloat(packetBuffer, bindValue);
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                errorMsg = bindToDouble(packetBuffer, bindValue);
                break;
            case BIT:
                errorMsg = bindToBit(packetBuffer, bindValue, clientCharset);
                break;
            case BOOLEAN:
            case TINYINT:
            case TINYINT_UNSIGNED:
                errorMsg = bindToTinyInt(packetBuffer, bindValue);
                break;
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case YEAR:
                errorMsg = bindShort(packetBuffer, bindValue);
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case VARCHAR:
            case CHAR:
            case SET:
            case JSON:
            case ENUM:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                // below binary
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
                errorMsg = bindToStringType(packetBuffer, bindValue, clientCharset);
                break;
            case TIME:
                errorMsg = bindToTime(packetBuffer, parameterMeta, bindValue);
                break;
            case DATE:
            case DATETIME:
            case TIMESTAMP:
                errorMsg = bindToDatetime(packetBuffer, parameterMeta, bindValue);
                break;
            case UNKNOWN:
            case GEOMETRY:
                errorMsg = null; //TODO add code
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(parameterMeta.mysqlType);
        }

        return errorMsg;
    }

    @Nullable
    private String bindToTinyInt(ByteBuf packetBuffer, BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        final int unsignedMaxByte = MySQLNumberUtils.unsignedByteToInt((byte) -1);
        if (nonNullValue instanceof Byte) {
            packetBuffer.writeByte((Byte) nonNullValue);
        } else if (nonNullValue instanceof Boolean) {
            packetBuffer.writeByte(((Boolean) nonNullValue) ? 1 : 0);
        } else if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (num >= Byte.MIN_VALUE && num <= unsignedMaxByte) {
                packetBuffer.writeByte(num);
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxByte, Byte.MIN_VALUE);
            }
        } else if (nonNullValue instanceof String) {
            try {
                int num = Integer.parseInt((String) nonNullValue);
                if (num >= Byte.MIN_VALUE && num <= unsignedMaxByte) {
                    packetBuffer.writeByte(num);
                } else {
                    errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxByte, Byte.MIN_VALUE);
                }
            } catch (NumberFormatException e) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof Short) {
            int num = (Short) nonNullValue;
            if (num >= Byte.MIN_VALUE && num <= unsignedMaxByte) {
                packetBuffer.writeByte(num);
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxByte, Byte.MIN_VALUE);
            }
        } else if (nonNullValue instanceof Long) {
            long num = (Long) nonNullValue;
            if (num >= Byte.MIN_VALUE && num <= unsignedMaxByte) {
                packetBuffer.writeByte((int) num);
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxByte, Byte.MIN_VALUE);
            }
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger num = (BigInteger) nonNullValue;
            if (num.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) >= 0
                    && num.compareTo(BigInteger.valueOf(unsignedMaxByte)) <= 0) {
                packetBuffer.writeByte(num.intValue());
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxByte, Byte.MIN_VALUE);
            }
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            } else if (num.compareTo(BigDecimal.valueOf(Byte.MIN_VALUE)) >= 0
                    && num.compareTo(BigDecimal.valueOf(unsignedMaxByte)) <= 0) {
                packetBuffer.writeByte(num.intValue());
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxByte, Byte.MIN_VALUE);
            }
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        return errorMsg;
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue, Charset)
     */
    @Nullable
    private String bindShort(ByteBuf packetBuffer, BindValue bindValue) {
        Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        final int unsignedMaxShort = MySQLNumberUtils.unsignedShortToInt((short) -1);
        if (nonNullValue instanceof Year) {
            PacketUtils.writeInt2(packetBuffer, ((Year) nonNullValue).getValue());
        } else if (nonNullValue instanceof Short) {
            PacketUtils.writeInt2(packetBuffer, ((Short) nonNullValue).intValue());
        } else if (nonNullValue instanceof Integer) {
            int num = (Integer) nonNullValue;
            if (num >= Short.MIN_VALUE && num <= unsignedMaxShort) {
                PacketUtils.writeInt2(packetBuffer, num);
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxShort, Short.MIN_VALUE);
            }
        } else if (nonNullValue instanceof String) {
            try {
                int num = Integer.parseInt((String) nonNullValue);
                if (num >= Short.MIN_VALUE && num <= unsignedMaxShort) {
                    PacketUtils.writeInt2(packetBuffer, num);
                } else {
                    errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxShort, Short.MIN_VALUE);
                }
            } catch (NumberFormatException e) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof Long) {
            long num = (Long) nonNullValue;
            if (num >= Short.MIN_VALUE && num <= unsignedMaxShort) {
                PacketUtils.writeInt2(packetBuffer, (int) num);
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxShort, Short.MIN_VALUE);
            }
        } else if (nonNullValue instanceof Byte) {
            PacketUtils.writeInt2(packetBuffer, ((Byte) nonNullValue).intValue());
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger num = (BigInteger) nonNullValue;
            if (num.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) >= 0
                    && num.compareTo(BigInteger.valueOf(unsignedMaxShort)) <= 0) {
                PacketUtils.writeInt2(packetBuffer, num.intValue());
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxShort, Short.MIN_VALUE);
            }
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            } else if (num.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) >= 0
                    && num.compareTo(BigDecimal.valueOf(unsignedMaxShort)) <= 0) {
                PacketUtils.writeInt2(packetBuffer, num.intValue());
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxShort, Short.MIN_VALUE);
            }
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        return errorMsg;
    }


    @Nullable
    private String bindToInt(ByteBuf packetBuffer, BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        final long unsignedMaxInt = MySQLNumberUtils.unsignedIntToLong(-1);
        if (nonNullValue instanceof Integer) {
            PacketUtils.writeInt4(packetBuffer, ((Integer) nonNullValue));
        } else if (nonNullValue instanceof Long) {
            long num = (Long) nonNullValue;
            if (num >= Integer.MIN_VALUE && num <= unsignedMaxInt) {
                PacketUtils.writeInt4(packetBuffer, (int) num);
            } else {
                errorMsg = createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof String) {
            try {
                long num = Long.parseLong((String) nonNullValue);
                if (num >= Short.MIN_VALUE && num <= unsignedMaxInt) {
                    PacketUtils.writeInt4(packetBuffer, (int) unsignedMaxInt);
                } else {
                    errorMsg = createNumberRangErrorMessage(bindValue, unsignedMaxInt, Integer.MIN_VALUE);
                }
            } catch (NumberFormatException e) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger num = (BigInteger) nonNullValue;
            if (num.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) >= 0
                    && num.compareTo(BigInteger.valueOf(unsignedMaxInt)) <= 0) {
                PacketUtils.writeInt4(packetBuffer, (int) num.longValue());
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue
                        , unsignedMaxInt
                        , BigInteger.valueOf(Integer.MIN_VALUE));
            }

        } else if (nonNullValue instanceof Short) {
            PacketUtils.writeInt4(packetBuffer, ((Short) nonNullValue).intValue());
        } else if (nonNullValue instanceof Byte) {
            PacketUtils.writeInt4(packetBuffer, ((Byte) nonNullValue).intValue());
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            if (num.scale() != 0) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            } else if (num.compareTo(BigDecimal.valueOf(Integer.MIN_VALUE)) >= 0
                    && num.compareTo(BigDecimal.valueOf(unsignedMaxInt)) <= 0) {
                PacketUtils.writeInt4(packetBuffer, (int) num.longValue());
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue
                        , unsignedMaxInt
                        , BigDecimal.valueOf(Integer.MIN_VALUE));
            }
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        return errorMsg;
    }

    @Nullable
    private String bindToFloat(ByteBuf packetBuffer, BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        if (nonNullValue instanceof Float) {
            PacketUtils.writeInt4(packetBuffer, Float.floatToIntBits((Float) nonNullValue));
        } else if (nonNullValue instanceof String) {
            try {
                PacketUtils.writeInt4(packetBuffer, Float.floatToIntBits(Float.parseFloat((String) nonNullValue)));
            } catch (NumberFormatException e) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof Short) {
            PacketUtils.writeInt4(packetBuffer, Float.floatToIntBits(((Short) nonNullValue).floatValue()));
        } else if (nonNullValue instanceof Byte) {
            PacketUtils.writeInt4(packetBuffer, Float.floatToIntBits(((Byte) nonNullValue).floatValue()));
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }

        return errorMsg;
    }


    @Nullable
    private String bindToBigInt(ByteBuf packetBuffer, BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        if (nonNullValue instanceof Long) {
            PacketUtils.writeInt8(packetBuffer, ((Long) nonNullValue));
        } else if (nonNullValue instanceof Integer) {
            PacketUtils.writeInt8(packetBuffer, ((Integer) nonNullValue).longValue());
        } else if (nonNullValue instanceof BigInteger) {
            BigInteger num = (BigInteger) nonNullValue;
            if (num.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0
                    && num.compareTo(MySQLNumberUtils.UNSIGNED_MAX_LONG) <= 0) {
                PacketUtils.writeInt8(packetBuffer, num);
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue
                        , MySQLNumberUtils.UNSIGNED_MAX_LONG
                        , BigInteger.valueOf(Long.MIN_VALUE));
            }

        } else if (nonNullValue instanceof Short) {
            PacketUtils.writeInt8(packetBuffer, ((Short) nonNullValue).longValue());
        } else if (nonNullValue instanceof Byte) {
            PacketUtils.writeInt8(packetBuffer, ((Byte) nonNullValue).longValue());
        } else if (nonNullValue instanceof String) {
            try {
                BigInteger num = new BigInteger((String) nonNullValue);
                if (num.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0
                        && num.compareTo(MySQLNumberUtils.UNSIGNED_MAX_LONG) <= 0) {
                    PacketUtils.writeInt8(packetBuffer, num);
                } else {
                    errorMsg = createNumberRangErrorMessage(bindValue
                            , MySQLNumberUtils.UNSIGNED_MAX_LONG
                            , BigInteger.valueOf(Long.MIN_VALUE));
                }
            } catch (NumberFormatException e) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof BigDecimal) {
            BigDecimal num = (BigDecimal) nonNullValue;
            BigDecimal unsignedMaxLong = new BigDecimal(MySQLNumberUtils.UNSIGNED_MAX_LONG);
            if (num.scale() != 0) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            } else if (num.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0
                    && num.compareTo(unsignedMaxLong) <= 0) {
                PacketUtils.writeInt8(packetBuffer, num.toBigInteger());
            } else {
                errorMsg = createNumberRangErrorMessage(bindValue
                        , unsignedMaxLong
                        , BigDecimal.valueOf(Long.MIN_VALUE));
            }
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        return errorMsg;
    }


    @Nullable
    private String bindToDouble(ByteBuf packetBuffer, BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        if (nonNullValue instanceof Double) {
            PacketUtils.writeInt8(packetBuffer, Double.doubleToLongBits((Double) nonNullValue));
        } else if (nonNullValue instanceof Float) {
            PacketUtils.writeInt8(packetBuffer, Double.doubleToLongBits((Float) nonNullValue));
        } else if (nonNullValue instanceof String) {
            try {
                PacketUtils.writeInt8(packetBuffer, Double.doubleToLongBits(Double.parseDouble((String) nonNullValue)));
            } catch (NumberFormatException e) {
                errorMsg = createTypeNotMatchMessage(bindValue);
            }
        } else if (nonNullValue instanceof Integer) {
            PacketUtils.writeInt8(packetBuffer, Double.doubleToLongBits(((Integer) nonNullValue).doubleValue()));
        } else if (nonNullValue instanceof Short) {
            PacketUtils.writeInt8(packetBuffer, Double.doubleToLongBits(((Short) nonNullValue).doubleValue()));
        } else if (nonNullValue instanceof Byte) {
            PacketUtils.writeInt8(packetBuffer, Double.doubleToLongBits(((Byte) nonNullValue).doubleValue()));
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }

        return errorMsg;
    }


    @Nullable
    private String bindToTime(ByteBuf packetBuffer, MySQLColumnMeta parameterMeta, BindValue bindValue) {
        final Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        final int length;
        if (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) {
            length = 8 + parameterMeta.decimals;
        } else {
            length = 8 + ((int) (parameterMeta.length - 20L));
        }
        final LocalTime time;
        if (nonNullValue instanceof LocalTime) {
            time = OffsetTime.of((LocalTime) nonNullValue, this.executorAdjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.executorAdjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNullValue instanceof OffsetTime) {
            time = ((OffsetTime) nonNullValue).withOffsetSameInstant(this.executorAdjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } else if (nonNullValue instanceof Duration) {
            time = null;
            Duration duration = (Duration) nonNullValue;
            packetBuffer.writeByte(length); //1. length

            packetBuffer.writeByte(duration.isNegative() ? 1 : 0); //2. is_negative
            duration = duration.abs();
            if (duration.getSeconds() > Constants.DURATION_MAX_SECONDS) {
                return String.format("Bind parameter[%s] MySQLType[%s] Duration[%s] beyond [-838:59:59,838:59:59]"
                        , bindValue.getIndex(), bindValue.getType(), duration);
            }
            long temp;
            temp = duration.toDays();
            PacketUtils.writeInt4(packetBuffer, (int) temp); //3. days
            duration = duration.minusDays(temp);

            temp = duration.toHours();
            packetBuffer.writeByte((int) temp); //4. hour
            duration = duration.minusHours(temp);

            temp = duration.toMinutes();
            packetBuffer.writeByte((int) temp); //5. minute
            duration = duration.minusMinutes(temp);

            temp = duration.getSeconds();
            packetBuffer.writeByte((int) temp); //6. second
            duration = duration.minusSeconds(temp);
            if (length == 12) {
                PacketUtils.writeInt4(packetBuffer, (int) duration.toMillis());//7, micro seconds
            }
        } else {
            time = null;
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        if (time != null) {
            packetBuffer.writeByte(length); //1. length
            packetBuffer.writeByte(0); //2. is_negative
            packetBuffer.writeZero(4); //3. days

            packetBuffer.writeByte(time.getHour()); //4. hour
            packetBuffer.writeByte(time.getMinute()); //5. minute
            packetBuffer.writeByte(time.getSecond()); ///6. second

            if (length == 11) {
                PacketUtils.writeInt4(packetBuffer, time.get(ChronoField.MICRO_OF_SECOND));//7, micro seconds
            }
        }
        return errorMsg;
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue, Charset)
     */
    @Nullable
    private String bindToDatetime(ByteBuf packetBuffer, MySQLColumnMeta parameterMeta, BindValue bindValue) {
        Object nonNullValue = bindValue.getRequiredValue();

        String errorMsg = null;
        LocalDateTime dateTime = null;
        if (nonNullValue instanceof LocalDate) {
            LocalDate date = (LocalDate) nonNullValue;

            packetBuffer.writeByte(4); // length
            PacketUtils.writeInt2(packetBuffer, date.getYear()); // year
            packetBuffer.writeByte(date.getMonthValue()); // month
            packetBuffer.writeByte(date.getDayOfMonth()); // day
        } else if (nonNullValue instanceof LocalDateTime) {
            dateTime = OffsetDateTime.of((LocalDateTime) nonNullValue, this.executorAdjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.executorAdjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof ZonedDateTime) {
            dateTime = ((ZonedDateTime) nonNullValue)
                    .withZoneSameInstant(this.executorAdjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else if (nonNullValue instanceof OffsetDateTime) {
            dateTime = ((OffsetDateTime) nonNullValue)
                    .withOffsetSameInstant(this.executorAdjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        if (dateTime != null) {
            final int length;
            if (parameterMeta.decimals > 0 && parameterMeta.decimals < 7) {
                length = 7 + parameterMeta.decimals;
            } else {
                length = 7 + ((int) (parameterMeta.length - 20L));
            }
            packetBuffer.writeByte(length); // length

            PacketUtils.writeInt2(packetBuffer, dateTime.getYear()); // year
            packetBuffer.writeByte(dateTime.getMonthValue()); // month
            packetBuffer.writeByte(dateTime.getDayOfMonth()); // day

            packetBuffer.writeByte(dateTime.getHour()); // hour
            packetBuffer.writeByte(dateTime.getMinute()); // minute
            packetBuffer.writeByte(dateTime.getSecond()); // second

            PacketUtils.writeInt4(packetBuffer, dateTime.get(ChronoField.MICRO_OF_SECOND));// micro second
        }
        return errorMsg;
    }


    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue, Charset)
     */
    @Nullable
    private String bindToBit(ByteBuf packetBuffer, BindValue bindValue, Charset clientCharset) {
        Object nonNullValue = bindValue.getRequiredValue();
        String errorMsg = null;
        if (nonNullValue instanceof Long) {
            PacketUtils.writeStringLenEnc(packetBuffer
                    , Long.toBinaryString((Long) nonNullValue).getBytes(clientCharset));
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        return errorMsg;
    }

    /**
     * @see #bindParameter(ByteBuf, MySQLColumnMeta, BindValue, Charset)
     */
    @Nullable
    private String bindToStringType(ByteBuf packetBuffer, BindValue bindValue, Charset clientCharset) {
        Object nonNullValue = Objects.requireNonNull(bindValue.getValue(), "bindValue");
        String errorMsg = null;
        if (nonNullValue instanceof String) {
            PacketUtils.writeStringLenEnc(packetBuffer, ((String) nonNullValue).getBytes(clientCharset));
        } else if (nonNullValue instanceof BigDecimal) {
            PacketUtils.writeStringLenEnc(packetBuffer
                    , ((BigDecimal) nonNullValue).toPlainString().getBytes(clientCharset));
        } else if (nonNullValue instanceof byte[]) {
            PacketUtils.writeStringLenEnc(packetBuffer, (byte[]) nonNullValue);
        } else if (nonNullValue instanceof Number) {
            PacketUtils.writeStringLenEnc(packetBuffer, nonNullValue.toString().getBytes(clientCharset));
        } else if (nonNullValue instanceof Character) {
            PacketUtils.writeStringLenEnc(packetBuffer, nonNullValue.toString().getBytes(clientCharset));
        } else if (nonNullValue instanceof Enum) {
            PacketUtils.writeStringLenEnc(packetBuffer, ((Enum<?>) nonNullValue).name().getBytes(clientCharset));
        } else if (nonNullValue instanceof Geometry) {
            // TODO add code
        } else {
            errorMsg = createTypeNotMatchMessage(bindValue);
        }
        return errorMsg;
    }

    private String createNumberRangErrorMessage(BindValue bindValue, Number upper, Number lower) {
        return String.format("Bind parameter[%s] MySQLType[%s] beyond rang[%s,%s]."
                , bindValue.getIndex(), bindValue.getType(), upper, lower);
    }


    private String createTypeNotMatchMessage(BindValue bindValue) {
        return String.format("Bind parameter[%s] MySQLType[%s] and JavaType[%s] not match."
                , bindValue.getIndex(), bindValue.getType(), bindValue.getRequiredValue().getClass().getName());
    }

    /*################################## blow private static method ##################################*/

    private int emitBindParameterTypeError(Class<?> parameterClass, final int parameterIndex) {
      /*  this.resultsSink.error(new BindParameterException(
                String.format("Bind parameter[%s] type[%s] error"
                        , parameterIndex, parameterClass.getName()), parameterIndex));*/
        closeStatement();
        return Integer.MIN_VALUE;
    }

    /*################################## blow private instance inner class ##################################*/

    private final class PublisherLongDataSubscriber implements CoreSubscriber<Object> {

        private final FluxSink<ByteBuf> sink;

        private final int parameterIndex;

        ByteBuf packetBuffer;

        private PublisherLongDataSubscriber(FluxSink<ByteBuf> sink, int parameterIndex) {
            this.sink = sink;
            this.parameterIndex = parameterIndex;
        }

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(final Object data) {
            ByteBuf packetBuffer = this.packetBuffer;
            if (data instanceof byte[]) {
                byte[] byteArray = (byte[]) data;
                if (packetBuffer == null) {
                    packetBuffer = createLongDataPacket(this.parameterIndex, byteArray.length);
                }
                this.packetBuffer = writeByteArrayToBlobPacket(byteArray, byteArray.length
                        , this.parameterIndex, packetBuffer, this.sink);
            } else if (data instanceof ByteBuffer) {
                ByteBuffer byteBuffer = (ByteBuffer) data;
                if (packetBuffer == null) {
                    packetBuffer = createLongDataPacket(this.parameterIndex, byteBuffer.remaining());
                }
                this.packetBuffer = writeByteBufferToBlobPacket(byteBuffer, this.parameterIndex, packetBuffer, this.sink);
            } else {
                this.sink.error(new BindParameterException(
                        String.format("Bind parameter[%s] type[%s] error.", this.parameterIndex, data.getClass())
                        , this.parameterIndex));
            }
        }

        @Override
        public void onError(Throwable t) {
            ByteBuf packetBuffer = this.packetBuffer;
            if (packetBuffer != null) {
                packetBuffer.release();
            }
            this.sink.error(new BindParameterException(
                    String.format("Bind parameter[%s]'s publisher throw error", this.parameterIndex)
                    , t, this.parameterIndex));
        }

        @Override
        public void onComplete() {
            ByteBuf packetBuffer = this.packetBuffer;
            if (packetBuffer != null && hasBlobData(packetBuffer)) {
                PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId()); // write header
                this.sink.next(packetBuffer); // send packet
                this.packetBuffer = null;
            }
        }


    }

    enum Phase {
        PREPARED,
        PREPARE_RESPONSE,
        READ_PARAM_META,
        READ_COLUMN_META,
        EXECUTE,
        RESET,
        READ_RESULT_SET
    }


}
