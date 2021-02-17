package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.session.ServerPreparedStatement;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.vendor.CommunicationTask;
import io.jdbd.vendor.TaskSignal;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * <p>
 *     <ol>
 *         <li>send COM_STMT_PREPARE : {@link #internalStart(TaskSignal)} </li>
 *         <li>read COM_STMT_PREPARE Response : {@link #readPrepareResponse(ByteBuf)}</li>
 *         <li>send COM_STMT_EXECUTE : {@link #createExecutionPacketPublisher(List)} </li>
 *         <li>read COM_STMT_EXECUTE Response : {@link #readExecuteResponse(ByteBuf, Consumer)}</li>
 *         <li>read Binary Protocol ResultSet Row : {@link #readExecuteBinaryRows(ByteBuf, Consumer)}</li>
 *     </ol>
 * </p>
 *
 * @see BinaryResultSetReader
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html">Prepared Statements</a>
 */
final class ComPreparedTask extends MySQLCommunicationTask implements PreparedStatementTask {

    static Mono<PreparedStatement> prepare(MySQLTaskAdjutant taskAdjutant, String sql) {
        return Mono.create(sink -> {
            // ComPreparedTask reference is hold by MySQLCommTaskExecutor.
            new ComPreparedTask(taskAdjutant, sql, sink)
                    .submit(sink::error);
        });
    }

    private static final int BUFFER_LENGTH = 8192;

    private static final int LONG_DATA_PREFIX_SIZE = 7;

    private final String sql;

    private final MonoSink<PreparedStatement> stmtSink;

    private final AtomicBoolean stmtCancel = new AtomicBoolean(false);

    private List<List<BindValue>> parameterGroupList;

    private int statementId;

    private int preparedWarningCount = 0;

    private boolean hasMeta = true;

    private Phase phase = Phase.PREPARED;

    private MySQLColumnMeta[] parameterMetas;

    private MySQLRowMeta rowMeta;

    private int columnMetaIndex = -1;

    private int parameterMetaIndex = -1;

    private int batchIndex = 0;

    private Publisher<ByteBuf> packetPublisher;

    private int cursorFetchSize = -1;

    private Throwable error;

    private TaskSignal<ByteBuf> signal;

    private FluxSink<ResultRow> rowSink;

    private Consumer<ResultStates> resultStatesConsumer;

    private MonoSink<ResultStates> updateSink;

    private Path bigRowPath;

    private ResultSetReader resultSetReader;

    private final int blobSendChunkSize;

    private final int maxBlobPacketSize;

    private ComPreparedTask(MySQLTaskAdjutant executorAdjutant, String sql, MonoSink<PreparedStatement> stmtSink) {
        super(executorAdjutant);
        this.sql = sql;
        this.stmtSink = stmtSink;
        this.blobSendChunkSize = obtainBlobSendChunkSize();
        this.maxBlobPacketSize = Math.min(PacketUtils.HEADER_SIZE + LONG_DATA_PREFIX_SIZE + this.blobSendChunkSize
                , PacketUtils.MAX_PACKET);
        this.stmtSink.onCancel(() -> this.stmtCancel.compareAndSet(false, true));
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
    public int obtainParameterCount() throws IllegalStateException {
        MySQLColumnMeta[] parameterMetaArray = this.parameterMetas;
        if (parameterMetaArray == null) {
            throw new IllegalStateException("Not prepared yet.");
        }
        return parameterMetaArray.length;
    }

    @Override
    public MySQLColumnMeta obtainParameterMeta(int parameterIndex) throws IllegalStateException {
        throw new UnsupportedOperationException();
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
        MySQLColumnMeta[] columnMetaArray = this.rowMeta.columnMetaArray;
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


    /*################################## blow protected  method ##################################*/

    @Override
    protected Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal) {
        assertPhase(Phase.PREPARED);
        this.signal = signal;
        // this method send COM_STMT_PREPARE packet.
        // @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
        int payloadLength = 1 + (this.sql.length() * this.executorAdjutant.obtainMaxBytesPerCharClient());
        ByteBuf packetBuffer = this.executorAdjutant.createPacketBuffer(payloadLength);

        packetBuffer.writeByte(PacketUtils.COM_STMT_PREPARE); // command
        packetBuffer.writeCharSequence(this.sql, this.executorAdjutant.obtainCharsetClient());
        PacketUtils.writePacketHeader(packetBuffer, addAndGetSequenceId());

        final Mono<ByteBuf> mono;
        if (this.stmtCancel.get()) {
            // downstream cancel subscription,terminate task
            this.phase = Phase.CLOSE_STMT;
            mono = null;
            packetBuffer.release();
            signal.terminate(this);
        } else {
            this.phase = Phase.READ_PREPARE_RESPONSE;
            mono = Mono.just(packetBuffer);
        }
        return mono;
    }

    @Override
    protected boolean internalDecode(final ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!PacketUtils.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        boolean taskEnd = false, continueDecode = true;
        while (continueDecode) {
            switch (this.phase) {
                case READ_PREPARE_RESPONSE: {
                    taskEnd = readPrepareResponse(cumulateBuffer);
                    continueDecode = !taskEnd;
                }
                break;
                case READ_PREPARE_PARAM_META: {
                    if (readPrepareParameterMeta(cumulateBuffer, serverStatusConsumer)) {
                        continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                        this.phase = Phase.READ_PREPARE_COLUMN_META;
                    } else {
                        continueDecode = false;
                    }
                }
                break;
                case READ_PREPARE_COLUMN_META: {
                    if (readPrepareColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                        continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                        markIdle();
                        this.stmtSink.success(ServerPreparedStatement.create(this));
                    } else {
                        continueDecode = false;
                    }
                }
                break;
                case EXECUTE: {
                    taskEnd = executeStatement();
                    if (!taskEnd) {
                        this.phase = Phase.READ_EXECUTE_RESPONSE;
                    }
                }
                break;
                case READ_EXECUTE_RESPONSE:
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    break;
                case READ_EXECUTE_COLUMN_META: {
                    if (readExecuteColumnMeta(cumulateBuffer)) {
                        this.phase = Phase.READ_EXECUTE_BINARY_ROW;
                        if (readExecuteBinaryRows(cumulateBuffer, serverStatusConsumer)) {
                            markIdle();
                        }
                    }
                }
                break;
                case READ_EXECUTE_BINARY_ROW: {
                    if (readExecuteBinaryRows(cumulateBuffer, serverStatusConsumer)) {
                        markIdle();
                    }
                }
                break;
                case PREPARED:
                    throw new IllegalStateException("this.phase is PREPARED ,task not start yet.");
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(this.phase);
            }
        }
        if (taskEnd && this.error != null) {
            emitErrorToResultDownstream(this.error);
            closeStatement();
        }
        return taskEnd;
    }

    /*################################## blow private method ##################################*/

    /**
     * @return true: prepare error,task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response">COM_STMT_PREPARE Response</a>
     */
    private boolean readPrepareResponse(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_PREPARE_RESPONSE);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        final int headFlag = PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex()); //1. status/error header
        boolean taskEnd;
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.executorAdjutant.obtainCharsetResults());
                this.stmtSink.error(MySQLExceptionUtils.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                final int payloadStartIndex = cumulateBuffer.readerIndex();
                cumulateBuffer.skipBytes(1);//skip status
                this.statementId = PacketUtils.readInt4(cumulateBuffer);//2. statement_id
                resetColumnMeta(PacketUtils.readInt2(cumulateBuffer));//3. num_columns
                resetParameterMetas(PacketUtils.readInt2(cumulateBuffer));//4. num_params
                cumulateBuffer.skipBytes(1); //5. skip filler
                this.preparedWarningCount = PacketUtils.readInt2(cumulateBuffer);//6. warning_count
                if ((this.negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
                    throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA"); //7. metadata_follows
                }
                cumulateBuffer.readerIndex(payloadStartIndex + payloadLength); // to next packet,avoid tail filler.
                this.phase = Phase.READ_PREPARE_PARAM_META;
                taskEnd = false;
            }
            break;
            default: {
                RuntimeException e = MySQLExceptionUtils.createFatalIoException(
                        "COM_STMT_PREPARE Response error. headFlag[%s]", headFlag);
                this.stmtSink.error(e);
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
     *         <li>{@link #parameterMetas}</li>
     *         <li>{@link #rowMeta}</li>
     *     </ul>
     * </p>
     *
     * @see #readPrepareResponse(ByteBuf, Consumer)
     */
    private void readPrepareMeta(ByteBuf cumulateBuffer, final int numColumns, final int numParams
            , Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_PREPARE_RESPONSE) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_PREPARE_RESPONSE));
        }
        // below read parameter meta and column meta.
        final boolean paramMetaReadEnd;
        if (numParams > 0) {
            resetParameterMetas(numParams);
            this.phase = Phase.READ_PREPARE_PARAM_META;
            paramMetaReadEnd = readPrepareParameterMeta(cumulateBuffer, serverStatusConsumer);
        } else {
            paramMetaReadEnd = true;
            this.parameterMetas = MySQLColumnMeta.EMPTY;
        }

        resetColumnMeta(numColumns);

        if (numColumns > 0 && paramMetaReadEnd) {
            this.phase = Phase.READ_PREPARE_COLUMN_META;
            if (readPrepareColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                this.phase = Phase.EXECUTE;
            }
        } else if (paramMetaReadEnd) {
            this.phase = Phase.EXECUTE;
        }
    }

    /**
     * @return true:read end
     * @see #readPrepareMeta(ByteBuf, int, int, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readPrepareParameterMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_PREPARE_PARAM_META);
        int parameterMetaIndex = this.parameterMetaIndex;
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
        if (parameterMetaIndex < metaArray.length) {
            parameterMetaIndex = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer
                    , parameterMetaIndex, this.executorAdjutant, metaArray, this::updateSequenceId);
            this.parameterMetaIndex = parameterMetaIndex;
        }
        return parameterMetaIndex == metaArray.length
                && tryReadEof(cumulateBuffer, serverStatusConsumer);
    }

    private void emitError(Throwable e) {
        if (this.error == null) {
            this.error = e;
        }
    }

    /**
     * @return true:read end
     * @see #readPrepareMeta(ByteBuf, int, int, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readPrepareColumnMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_PREPARE_COLUMN_META) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_PREPARE_COLUMN_META));
        }
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.rowMeta.columnMetaArray, "this.columnIndex");
        int columnMetaIndex = this.columnMetaIndex;
        if (columnMetaIndex < metaArray.length) {
            columnMetaIndex = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer, columnMetaIndex, this.executorAdjutant
                    , metaArray, this::updateSequenceId);
            this.columnMetaIndex = columnMetaIndex;
        }
        return columnMetaIndex == metaArray.length
                && tryReadEof(cumulateBuffer, serverStatusConsumer);
    }

    /**
     * @return false : need more cumulate
     * @see #readPrepareParameterMeta(ByteBuf, Consumer)
     * @see #readPrepareColumnMeta(ByteBuf, Consumer)
     */
    private boolean tryReadEof(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        boolean end = true;
        if ((this.negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) == 0) {
            if (PacketUtils.hasOnePacket(cumulateBuffer)) {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
                EofPacket eof;
                eof = EofPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(eof.getStatusFags());
            } else {
                end = false;
            }
        }
        return end;
    }

    /**
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">COM_STMT_EXECUTE</a>
     */
    private void executeStatement() {
        assertPhase(Phase.EXECUTE);

        final int batchIndex = this.batchIndex;
        final List<List<BindValue>> parameterGroupList = Objects.requireNonNull(
                this.parameterGroupList, "this.parameterGroupList");
        if (batchIndex == parameterGroupList.size()) {
            // no more param group,task end
            return;
        } else if (batchIndex > parameterGroupList.size()) {
            throw new IllegalStateException(String.format("Batch index[%s] > %s ."
                    , batchIndex, parameterGroupList.size()));
        }
        this.batchIndex++;
        final List<BindValue> parameterGroup = parameterGroupList.get(batchIndex);
        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");

        final int parameterCount = parameterGroup.size();
        if (parameterCount != parameterMetaArray.length) {
            SQLBindParameterException e = new SQLBindParameterException(
                    String.format("Bind batch[%s] parameter size[%s] and prepare parameter size[%s]"
                            , batchIndex, parameterCount, parameterMetaArray.length
                    ));
            markIdle();
            emitErrorToResultDownstream(e);
            return;
        }
        updateSequenceId(-1); // reset sequence_id for COM_STMT_EXECUTE protocol
        if (parameterCount == 0) {
            this.packetPublisher = Mono.just(createExecutePacketBuffer(10));
        } else {
            this.packetPublisher = createExecutePacketPublisherWithParameters(parameterGroup);
        }
    }



    private void closeStatement() {

    }

    @Nullable
    private SQLBindParameterException checkBindParameterType(MySQLColumnMeta parameterMeta, MySQLType bindType
            , Object nonNullValue) {
        return null;
    }

    /**
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readExecuteResponse(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_EXECUTE_RESPONSE) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't %s."
                    , this.phase, Phase.READ_EXECUTE_RESPONSE));
        }
        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1(cumulateBuffer));
        final int header = PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex());
        boolean taskEnd = false;
        switch (header) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.executorAdjutant.obtainCharsetResults());
                emitErrorToResultDownstream(MySQLExceptionUtils.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                OkPacket ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                emitUpdateResult(ok);
                serverStatusConsumer.accept(ok.getStatusFags());
                markIdle();
            }
            break;
            default: {
                // column_count
                final int payloadStartIndex = cumulateBuffer.readerIndex();
                final int columnCount = PacketUtils.readLenEncAsInt(cumulateBuffer);
                cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);//to next packet,avoid tail filler

                if (columnCount != this.rowMeta.columnMetaArray.length && this.error == null) {
                    this.error = new JdbdSQLException("Read binary result set error."
                            , new SQLException("Column metadata length of COM_STMT_PREPARE and Column metadata length of COM_STMT_EXECUTE not match."));
                }
                //update column metas
                resetColumnMeta(columnCount);
                this.phase = Phase.READ_EXECUTE_COLUMN_META;
                if (!readExecuteColumnMeta(cumulateBuffer)) {
                    break;
                }
                this.phase = Phase.READ_EXECUTE_BINARY_ROW;
                if (readExecuteBinaryRows(cumulateBuffer, serverStatusConsumer)) {
                    markIdle();
                }

            }
        }
        return taskEnd;
    }

    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     */
    private void emitErrorToResultDownstream(Throwable e) {
        if (this.resultStatesConsumer == null) {
            Objects.requireNonNull(this.updateSink, "this.updateSink").error(e);
        } else {
            Objects.requireNonNull(this.rowSink, "this.updateSink").error(e);
        }
    }

    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     */
    private void emitUpdateResult(OkPacket ok) {
        Object sink = this.rowSink;
        if (sink == null) {
            throw new NullPointerException("this.resultSink is null");
        } else if (sink instanceof MonoSink) {
            ((MonoSink<MySQLResultStates>) sink).success(MySQLResultStates.from(ok));
        } else if (sink instanceof FluxSink) {
            ((FluxSink<?>) sink).error(SubscriptionNotMatchException.expectUpdate());
        } else {
            throw new IllegalStateException(String.format("this.sink type[%s] unknown.", sink.getClass().getName()));
        }
    }

    /**
     * @see #readExecuteBinaryRows(ByteBuf, Consumer)
     */
    private void consumeQueryResultStatus(ResultStates resultStates) {
        Consumer<ResultStates> consumer = this.resultStatesConsumer;
        if (consumer == null) {
            throw new NullPointerException("this.resultStatesConsumer is null");
        } else {
            consumer.accept(resultStates);
        }
    }

    private void markIdle() {
        this.phase = Phase.IDLE;
    }

    /**
     * @return true: read execute column meta finish.
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readExecuteColumnMeta(ByteBuf cumulateBuffer) {
        if (this.phase != Phase.READ_EXECUTE_COLUMN_META) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_EXECUTE_COLUMN_META));
        }
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.rowMeta.columnMetaArray, "this.rowMeta.columnMetas");
        int columnMetaIndex = this.columnMetaIndex;
        if (columnMetaIndex < metaArray.length) {
            columnMetaIndex = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer, columnMetaIndex
                    , this.executorAdjutant, metaArray, this::updateSequenceId);
            this.columnMetaIndex = columnMetaIndex;
        }
        return columnMetaIndex == metaArray.length;
    }

    /**
     * @return true: read EOF_Packet of Binary Protocol ResultSet,it mean ResultSet terminate.
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readExecuteBinaryRows(final ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_EXECUTE_BINARY_ROW) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_EXECUTE_BINARY_ROW));
        }
        final ResultSetReader resultSetReader = this.resultSetReader;
        if (resultSetReader == null) {
            throw new NullPointerException("this.resultSetReader");
        }
        return resultSetReader.read(cumulateBuffer, serverStatusConsumer);
    }


    /**
     * @see #readPrepareMeta(ByteBuf, int, int, boolean, Consumer)
     * @see #readExecuteResponse(ByteBuf, Consumer)
     */
    private void resetColumnMeta(int columnCount) {
        if (columnCount == 0) {
            this.rowMeta = MySQLRowMeta.EMPTY;
        } else {
            this.rowMeta = MySQLRowMeta.from(new MySQLColumnMeta[columnCount]
                    , this.executorAdjutant.obtainCustomCollationMap());
        }
        this.columnMetaIndex = 0;
    }

    /**
     * @see #readPrepareMeta(ByteBuf, int, int, boolean, Consumer)
     */
    private void resetParameterMetas(int parameterCount) {
        this.parameterMetas = new MySQLColumnMeta[parameterCount];
        this.parameterMetaIndex = 0;
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



    /*################################## blow private method ##################################*/

    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", executorAdjutant));
        }
    }

    /*################################## blow private static method ##################################*/


    enum Phase {
        PREPARED,
        READ_PREPARE_RESPONSE,
        READ_PREPARE_PARAM_META,
        READ_PREPARE_COLUMN_META,

        EXECUTE,
        READ_EXECUTE_RESPONSE,
        READ_EXECUTE_COLUMN_META,
        READ_EXECUTE_BINARY_ROW,

        RESET_STMT,
        FETCH_RESULT,
        READ_FETCH_RESULT,
        IDLE,
        CLOSE_STMT
    }


}
