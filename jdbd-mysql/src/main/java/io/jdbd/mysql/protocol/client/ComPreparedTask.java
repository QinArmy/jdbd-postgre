package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.CommunicationTask;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
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

    private static final int MAX_DATA = ClientProtocol.MAX_PACKET_SIZE - 7;


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

    private ComPreparedTask(MySQLTaskAdjutant executorAdjutant, String sql, MonoSink<PreparedTask> taskSink) {
        super(executorAdjutant);
        this.sql = sql;
        this.taskSink = taskSink;
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
     * @return true : task end because bind parameter occur error.
     * @throws IllegalArgumentException when {@link BindValue#getValue()} is null.
     * @see #createExecutionPacketPublisher(List)
     */
    @Nullable
    private String bindParameter(ByteBuf packetBuffer, MySQLColumnMeta parameterMeta, BindValue bindValue) {
        final Object nonNullValue = bindValue.getValue();
        if (nonNullValue == null) {
            throw new IllegalArgumentException("bindValue.getValue() is null ");
        }
        String errorMsg;
        switch (parameterMeta.mysqlType) {
            case INT:
                errorMsg = bindToInt(packetBuffer, nonNullValue, bindValue.getType());
                break;
            case INT_UNSIGNED:
                errorMsg = bindToUnsignedInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case BIGINT:
                errorMsg = bindToBigInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case BIGINT_UNSIGNED:
                errorMsg = bindToUnsignedBigInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case FLOAT:
                errorMsg = bindToFloat(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case FLOAT_UNSIGNED:
                errorMsg = bindToUnsignedFloat(nonNullValue, parameterMeta, bindValue.getType());
                break;

            case DOUBLE:
                errorMsg = bindToDouble(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case DOUBLE_UNSIGNED:
                errorMsg = bindToUnsignedDouble(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case BIT:
                errorMsg = bindToBit(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case BOOLEAN:
                errorMsg = bindToBoolean(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case TINYINT:
                errorMsg = bindToTinyInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case TINYINT_UNSIGNED:
                errorMsg = bindToUnsignedTinyInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case SMALLINT:
                errorMsg = bindToSmallInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case SMALLINT_UNSIGNED:
                errorMsg = bindToUnsignedSmallInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case MEDIUMINT:
                errorMsg = bindToMediumInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case MEDIUMINT_UNSIGNED:
                errorMsg = bindToUnsignedMediumInt(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case DECIMAL:
                errorMsg = bindToDecimal(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case DECIMAL_UNSIGNED:
                errorMsg = bindToUnsignedDecimal(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case VARCHAR:
                errorMsg = bindToVarChar(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case CHAR:
                errorMsg = bindToChar(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case SET:
            case JSON:
            case ENUM:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                errorMsg = null;
                break;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
                errorMsg = null;
                break;
            case TIME:
                errorMsg = bindToTime(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case DATE:
                errorMsg = bindToDate(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case DATETIME:
                errorMsg = bindToDatetime(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case YEAR:
                errorMsg = bindToYear(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case TIMESTAMP:
                errorMsg = bindToTimestamp(nonNullValue, parameterMeta, bindValue.getType());
                break;
            case NULL:
            case UNKNOWN:
            case GEOMETRY:
                errorMsg = null;
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(parameterMeta.mysqlType);
        }

        return errorMsg;
    }

    private Publisher<ByteBuf> createResetPacketPublisher() {
        ByteBuf packetBuffer = this.executorAdjutant.createPacketBuffer(5);

        packetBuffer.writeByte(PacketUtils.COM_STMT_RESET);
        PacketUtils.writeInt4(packetBuffer, this.statementId);
        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());

        return Mono.just(packetBuffer);
    }


    /**
     * @see #executeStatement()
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
    @SuppressWarnings("unchecked")
    private Flux<ByteBuf> sendLongData(Pair<Integer, Object> pair) {
        final Object value = Objects.requireNonNull(pair.getSecond(), "pair.getSecond()");
        Flux<ByteBuf> flux;
        if (value instanceof byte[]) {
            flux = sendByteArrayLongData(pair.getFirst(), (byte[]) value);
        } else if (value instanceof InputStream) {
            flux = sendInputStreamLongData(pair.getFirst(), (InputStream) value);
        } else if (value instanceof Reader) {
            flux = sendReaderLongData(pair.getFirst(), (Reader) value);
        } else if (value instanceof Path) {
            flux = sendPathLongData(pair.getFirst(), (Path) value);
        } else if (value instanceof Publisher) {
            flux = sendPublisherLongData(pair.getFirst(), (Publisher<byte[]>) value);
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

            for (int i = 1, offset = 0, length = MAX_DATA; i <= time; ) {
                // below write payload length
                if (i < time) {
                    PacketUtils.writeInt3(packetBuffer, ClientProtocol.MAX_PACKET_SIZE);
                } else {
                    PacketUtils.writeInt3(packetBuffer, length);
                }
                PacketUtils.writeInt1(packetBuffer, addAndGetSequenceId()); // write sequence_id;
                if (i == 1) {
                    packetBuffer.writeByte(PacketUtils.COM_STMT_SEND_LONG_DATA); //status
                    PacketUtils.writeInt4(packetBuffer, this.statementId); //statement_id
                    PacketUtils.writeInt2(packetBuffer, paramId);//param_id
                }
                if (offset < longData.length) {
                    packetBuffer.writeBytes(longData, offset, length);
                }

                i++;
                offset += length;
                length = longData.length - offset;
            }
        }
        return Flux.just(packetBuffer);
    }

    /**
     * @param longData <ul>
     *                 <li>{@link InputStream}</li>
     *                 <li>{@link ScatteringByteChannel}</li>
     *                 </ul>
     * @see #sendLongData(Pair)
     */
    private Flux<ByteBuf> sendInputStreamLongData(final int paramId, Closeable longData) {
        return Flux.create(sink -> {
//            MySQLColumnMeta[] parameterMetaArray = this.parameterMetas;
//            MySQLColumnMeta parameterMeta = parameterMetaArray[parameterIndex];
            try {
                ByteBuf packetBuffer = createLongDataPacket(paramId, 1024);
                int len, dataLen = packetBuffer.writableBytes(), capacity;
                while (true) {
                    if (longData instanceof InputStream) {
                        len = packetBuffer.writeBytes((InputStream) longData, dataLen);
                    } else if (longData instanceof ScatteringByteChannel) {
                        len = packetBuffer.writeBytes((ScatteringByteChannel) longData, dataLen);
                    } else {
                        sink.error(new BindParameterException(
                                String.format("Bind parameter[%s] type[%s] not support"
                                        , paramId, longData.getClass().getName()), paramId));
                        break;
                    }

                    if (len < dataLen) {
                        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());
                        sink.next(packetBuffer);
                        break;
                    }
                    if (packetBuffer.capacity() == PacketUtils.MAX_PACKET_CAPACITY) {
                        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());
                        sink.next(packetBuffer);
                        packetBuffer = this.executorAdjutant.createPacketBuffer(1024);
                    } else {
                        capacity = Math.min(packetBuffer.capacity() * 2, PacketUtils.MAX_PACKET_CAPACITY);
                        packetBuffer = packetBuffer.capacity(capacity);
                    }
                    dataLen = packetBuffer.writableBytes();
                }

            } catch (IOException e) {
                sink.error(new BindParameterException(
                        String.format("Bind parameter[%s] read error.", paramId), e, paramId));
            }
        });
    }

    private Flux<ByteBuf> sendReaderLongData(final int paramId, Reader longData) {
        return Flux.create(sink -> {
            MySQLColumnMeta[] parameterMetaArray = this.parameterMetas;
            final MySQLColumnMeta parameterMeta = parameterMetaArray[paramId];
            try {
                ByteBuf packetBuffer = createLongDataPacket(paramId, 1024);
                int len, dataLen = packetBuffer.writableBytes(), capacity, resetCapacity;

                final Charset clobCharset = obtainClobCharset();
                CharsetEncoder encoder = clobCharset.newEncoder();
                CharBuffer charBuffer = CharBuffer.allocate(1024);
                int maxBytesPerChar = (int) Math.ceil(encoder.maxBytesPerChar());
                if (clobCharset.equals(StandardCharsets.UTF_16) && maxBytesPerChar == 1) {
                    maxBytesPerChar = 2;// for safety
                }
                ByteBuffer byteBuffer = ByteBuffer.allocate(maxBytesPerChar);
                CoderResult coderResult;
                while (true) {
                    len = longData.read(charBuffer);
                    coderResult = encoder.encode(charBuffer, byteBuffer, true);
                    if (coderResult.isError()) {
                        sink.error(createBindParameterException(paramId, coderResult));
                        break;
                    }
                    dataLen = byteBuffer.remaining();
                    if (dataLen <= packetBuffer.writableBytes()) {
                        packetBuffer.writeBytes(byteBuffer);
                    } else {
                        packetBuffer = adjustsOnePacketCapacity(packetBuffer, dataLen);
                        resetCapacity = packetBuffer.writableBytes();
                        if (resetCapacity >= dataLen) {
                            packetBuffer.writeBytes(byteBuffer);
                        } else {
                            final int originalLimit = byteBuffer.limit();
                            byteBuffer.limit(byteBuffer.position() + resetCapacity);
                            packetBuffer.writeBytes(byteBuffer);
                            byteBuffer.limit(originalLimit);
                        }
                    }
                    if (byteBuffer.capacity() == PacketUtils.MAX_PACKET_CAPACITY && !packetBuffer.isWritable()) {
                        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());
                        sink.next(packetBuffer);
                        packetBuffer = this.executorAdjutant.createPacketBuffer(1024);
                    }
                    if (len < dataLen) {
                        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());
                        sink.next(packetBuffer);
                        break;
                    }
                    if (packetBuffer.capacity() == PacketUtils.MAX_PACKET_CAPACITY) {
                        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());
                        sink.next(packetBuffer);
                        packetBuffer = this.executorAdjutant.createPacketBuffer(1024);
                    } else {
                        capacity = Math.min(packetBuffer.capacity() * 2, PacketUtils.MAX_PACKET_CAPACITY);
                        packetBuffer = packetBuffer.capacity(capacity);
                    }
                    dataLen = packetBuffer.writableBytes();
                }

            } catch (IOException e) {
                sink.error(new BindParameterException(
                        String.format("Bind parameter[%s] read error.", paramId), e, paramId));
            }
        });
    }

    private Flux<ByteBuf> sendPathLongData(int parameterIndex, Path path) {
        return Flux.empty();
    }

    private Flux<ByteBuf> sendPublisherLongData(int parameterIndex, Publisher<byte[]> longData) {
        return Flux.empty();
    }

    private ByteBuf createLongDataPacket(int parameterIndex, int initialPayloadCapacity) {
        if (initialPayloadCapacity < 1) {
            initialPayloadCapacity = 1024;
        }
        ByteBuf packetBuffer = this.executorAdjutant.createPacketBuffer(7 + initialPayloadCapacity);

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

    private BindParameterException createBindParameterException(final int paramId, CoderResult coderResult) {
        BindParameterException ex;
        try {
            coderResult.throwException();
            // never here.
            ex = new BindParameterException("", paramId);
        } catch (CharacterCodingException e) {
            ex = new BindParameterException(String.format("Parameter[%s] encode error.", paramId), e, paramId);
        }
        return ex;
    }

    private ByteBuf adjustsOnePacketCapacity(ByteBuf packetBuffer, final int dataLen) {
        final int needCapacity = ((packetBuffer.capacity() - PacketUtils.HEADER_SIZE) << 1) + PacketUtils.HEADER_SIZE;
        final int newCapacity;
        final ByteBuf newBuffer;
        newCapacity = Math.min(needCapacity, PacketUtils.MAX_PACKET_CAPACITY);
        if (packetBuffer.maxCapacity() < newCapacity) {
            newBuffer = packetBuffer.alloc().buffer(newCapacity);
            newBuffer.writeBytes(packetBuffer);
            packetBuffer.release();
        } else {
            packetBuffer.capacity(newCapacity);
            newBuffer = packetBuffer;
        }
        return newBuffer;
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
            String errorMsg = bindParameter(packetBuffer, parameterMetaArray[i], bindValue);
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

    @Nullable
    private String bindToBoolean(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToTinyInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToUnsignedTinyInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToSmallInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToUnsignedSmallInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToMediumInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToUnsignedMediumInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToInt(ByteBuf packetBuffer, Object nonNullValue, MySQLType bindType) {
        switch (bindType) {
            case INT:
                PacketUtils.writeInt4(packetBuffer, (Integer) nonNullValue);
                break;
            case INT_UNSIGNED:
            case MEDIUMINT_UNSIGNED:
            case SMALLINT_UNSIGNED:
            case TINYINT_UNSIGNED:
            case BIGINT_UNSIGNED:
            case SMALLINT:
            case TINYINT:
            case MEDIUMINT:
            case BOOLEAN:
        }
        return null;
    }

    @Nullable
    private String bindToUnsignedInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToFloat(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToUnsignedFloat(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToBigInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToUnsignedBigInt(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToDouble(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToUnsignedDouble(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToDecimal(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToUnsignedDecimal(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToTime(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToDate(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToDatetime(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToTimestamp(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToYear(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToBit(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToChar(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToVarChar(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    @Nullable
    private String bindToSet(Object nonNullValue, MySQLColumnMeta parameterMeta, MySQLType bindType) {
        return null;
    }

    /*################################## blow private static method ##################################*/

    private int emitBindParameterTypeError(Class<?> parameterClass, final int parameterIndex) {
      /*  this.resultsSink.error(new BindParameterException(
                String.format("Bind parameter[%s] type[%s] error"
                        , parameterIndex, parameterClass.getName()), parameterIndex));*/
        closeStatement();
        return Integer.MIN_VALUE;
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
