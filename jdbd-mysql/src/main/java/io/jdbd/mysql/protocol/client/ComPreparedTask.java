package io.jdbd.mysql.protocol.client;

import io.jdbd.PreparedStatement;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.session.ServerPreparedStatement;
import io.jdbd.mysql.util.MySQLCollectionUtils;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * <p>  code navigation :
 *     <ol>
 *         <li>decode entrance method : {@link #internalDecode(ByteBuf, Consumer)}</li>
 *         <li>send COM_STMT_PREPARE : {@link #internalStart(TaskSignal)} </li>
 *         <li>read COM_STMT_PREPARE Response : {@link #readPrepareResponse(ByteBuf)}
 *              <ol>
 *                  <li>read parameter meta : {@link #readPrepareParameterMeta(ByteBuf, Consumer)}</li>
 *                  <li>read prepare column meta : {@link #readPrepareColumnMeta(ByteBuf, Consumer)}</li>
 *              </ol>
 *         </li>
 *         <li>send COM_STMT_EXECUTE :
 *              <ul>
 *                  <li>{@link #executeQueryStatement(FluxSink, List, Consumer)}</li>
 *                  <li>{@link #executeUpdateStatement(MonoSink, List)}</li>
 *                  <li>{@link #executeBatchUpdateStatement(FluxSink, List)}</li>
 *                  <li> {@link PrepareExecuteCommandWriter#writeCommand(List)}</li>
 *              </ul>
 *         </li>
 *         <li>read COM_STMT_EXECUTE Response : {@link #readExecuteResponse(ByteBuf, Consumer)}</li>
 *         <li>read Binary Protocol ResultSet Row : {@link #readExecuteBinaryRows(ByteBuf, Consumer)}</li>
 *     </ol>
 * </p>
 *
 * @see PrepareExecuteCommandWriter
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

    private int parameterMetaIndex = -1;

    private MySQLColumnMeta[] prepareColumnMetas;

    private int columnMetaIndex = -1;


    private int batchIndex = 0;

    private Publisher<ByteBuf> packetPublisher;

    private int cursorFetchSize = -1;

    private final List<Throwable> errorList = new ArrayList<>();

    private TaskSignal<ByteBuf> signal;

    private FluxSink<ResultRow> querySink;

    private Consumer<ResultStates> resultStatesConsumer;

    private MonoSink<ResultStates> updateSink;

    private FluxSink<ResultStates> batchUpdateSink;

    private ResultSetReader resultSetReader;

    private StatementCommandWriter executeCommandWriter;

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
        throw new UnsupportedOperationException();
    }

    @Override
    public Flux<ResultRow> executeQuery(List<BindValue> parameterGroup, Consumer<ResultStates> statesConsumer) {
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                executeQueryInEventLoop(sink, MySQLCollectionUtils.asModifiableList(parameterGroup), statesConsumer);
            } else {
                this.adjutant.execute(() ->
                        executeQueryInEventLoop(sink, MySQLCollectionUtils.asModifiableList(parameterGroup)
                                , statesConsumer)
                );
            }
        });
    }

    @Override
    public Mono<ResultStates> executeUpdate(final List<BindValue> parameterGroup) {
        return Mono.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                executeUpdateInEventLoop(sink, MySQLCollectionUtils.asModifiableList(parameterGroup));
            } else {
                this.adjutant.execute(() ->
                        executeUpdateInEventLoop(sink, MySQLCollectionUtils.asModifiableList(parameterGroup))
                );
            }
        });
    }

    @Override
    public Flux<ResultStates> executeBatchUpdate(final List<List<BindValue>> parameterGroupList) {
        return Flux.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                executeBatchUpdateInEventLoop(sink, MySQLCollectionUtils.asModifiableList(parameterGroupList));
            } else {
                this.adjutant.execute(() ->
                        executeBatchUpdateInEventLoop(sink, MySQLCollectionUtils.asModifiableList(parameterGroupList))
                );
            }
        });
    }


    /*################################## blow protected  method ##################################*/

    @Override
    protected Publisher<ByteBuf> internalStart(TaskSignal<ByteBuf> signal) {
        assertPhase(Phase.PREPARED);
        this.signal = signal;
        // this method send COM_STMT_PREPARE packet.
        // @see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
        int payloadLength = 1 + (this.sql.length() * this.adjutant.obtainMaxBytesPerCharClient());
        ByteBuf packetBuffer = this.adjutant.createPacketBuffer(payloadLength);

        packetBuffer.writeByte(PacketUtils.COM_STMT_PREPARE); // command
        packetBuffer.writeCharSequence(this.sql, this.adjutant.obtainCharsetClient());
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

    /**
     * @see #decode(ByteBuf, Consumer)
     */
    @Override
    protected boolean internalDecode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
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
                        this.phase = Phase.READ_PREPARE_COLUMN_META;
                        continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                    } else {
                        continueDecode = false;
                    }
                }
                break;
                case READ_PREPARE_COLUMN_META: {
                    if (readPrepareColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                        continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                        markIdle(); // idle wait for prepare bind parameter .
                        this.stmtSink.success(ServerPreparedStatement.create(this));
                    } else {
                        continueDecode = false;
                    }
                }
                break;
                case READ_EXECUTE_RESPONSE: {
                    // maybe modify this.phase
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd;
                }
                break;
                case READ_RESULT_SET: {
                    taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
                    continueDecode = !taskEnd;
                }
                break;
                case PREPARED:
                case EXECUTE:
                case CLOSE_STMT:
                    throw new IllegalStateException(String.format("this.phase[%s] error.", this.phase));
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(this.phase);
            }
        }
        if (taskEnd) {

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
                        , this.negotiatedCapability, this.adjutant.obtainCharsetResults());
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
                        "Server send COM_STMT_PREPARE Response error. header        [%s]", headFlag);
                this.stmtSink.error(e);
                throw e;
            }
        }

        return taskEnd;
    }


    /**
     * @return true:read parameter meta end.
     * @see #readPrepareResponse(ByteBuf)
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #resetParameterMetas(int)
     */
    private boolean readPrepareParameterMeta(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_PREPARE_PARAM_META);
        int parameterMetaIndex = this.parameterMetaIndex;
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
        if (parameterMetaIndex < metaArray.length) {
            parameterMetaIndex = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer
                    , parameterMetaIndex, this.adjutant, metaArray, this::updateSequenceId);
            this.parameterMetaIndex = parameterMetaIndex;
        }
        return parameterMetaIndex == metaArray.length && tryReadEof(cumulateBuffer, serverStatusConsumer);
    }


    /**
     * @return true:read column meta end.
     * @see #readPrepareColumnMeta(ByteBuf, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #resetColumnMeta(int)
     */
    private boolean readPrepareColumnMeta(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (this.phase != Phase.READ_PREPARE_COLUMN_META) {
            throw new IllegalStateException(
                    String.format("this.phase[%s] isn't %s.", this.phase, Phase.READ_PREPARE_COLUMN_META));
        }

        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.prepareColumnMetas, "this.prepareColumnMetas");
        int columnMetaIndex = this.columnMetaIndex;
        if (columnMetaIndex < metaArray.length) {
            columnMetaIndex = AbstractComQueryTask.tryReadColumnMetas(cumulateBuffer, columnMetaIndex
                    , this.adjutant, metaArray, this::updateSequenceId);
            this.columnMetaIndex = columnMetaIndex;
        }
        return columnMetaIndex == metaArray.length && tryReadEof(cumulateBuffer, serverStatusConsumer);
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
     */
    private void closeStatement() {

    }


    /**
     * @see #executeUpdate(List)
     */
    private void executeUpdateInEventLoop(final MonoSink<ResultStates> sink, final List<BindValue> parameterGroup) {
        switch (this.phase) {
            case CLOSE_STMT:
                sink.error(createStatementCloseException());
                break;
            case WAIT_FOR_PARAMETER: {
                if (this.querySink != null || this.updateSink != null || this.batchUpdateSink != null) {
                    throw createExecuteMultiTimeException();
                }
                executeUpdateStatement(sink, parameterGroup);
            }
            break;
            default:
                sink.error(createExecuteMultiTimeException());
        }
    }

    /**
     * @see #executeUpdateInEventLoop(MonoSink, List)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private void executeUpdateStatement(final MonoSink<ResultStates> sink, final List<BindValue> parameterGroup) {
        this.phase = Phase.EXECUTE;
        this.updateSink = sink;
        this.batchUpdateSink = null;

        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(
                this.parameterMetas, "this.parameterMetas");

        updateSequenceId(-1); // reset sequence_id
        final StatementCommandWriter commandWriter = new PrepareExecuteCommandWriter(
                this.statementId, parameterMetaArray
                , false, this::addAndGetSequenceId
                , this.adjutant, this::executeCommandWriterErrorEvent);
        this.executeCommandWriter = commandWriter;

        if (parameterGroup.isEmpty()) {
            this.parameterGroupList = Collections.emptyList();
            this.batchIndex = -1;
        } else {
            this.parameterGroupList = Collections.singletonList(parameterGroup);
            this.batchIndex = 0;
        }

        // write execute command.

        this.packetPublisher = commandWriter.writeCommand(parameterGroup);
        this.phase = Phase.READ_EXECUTE_RESPONSE;

        // send signal to task executor.
        Objects.requireNonNull(this.signal, "this.signal").sendPacket(this);

    }


    /**
     * @see #executeQuery(List, Consumer)
     */
    private void executeQueryInEventLoop(final FluxSink<ResultRow> sink, final List<BindValue> parameterGroup
            , final Consumer<ResultStates> statesConsumer) {
        switch (this.phase) {
            case CLOSE_STMT:
                sink.error(createStatementCloseException());
                break;
            case WAIT_FOR_PARAMETER: {
                if (this.querySink != null || this.updateSink != null || this.batchUpdateSink != null) {
                    throw createExecuteMultiTimeException();
                }
                executeQueryStatement(sink, parameterGroup, statesConsumer);
            }
            break;
            default:
                sink.error(createExecuteMultiTimeException());
        }
    }

    /**
     * @see #executeQueryInEventLoop(FluxSink, List, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #readExecuteResponse(ByteBuf, Consumer)
     */
    private void executeQueryStatement(final FluxSink<ResultRow> sink, final List<BindValue> parameterGroup
            , final Consumer<ResultStates> statesConsumer) {
        this.phase = Phase.EXECUTE;
        this.querySink = sink;
        this.resultStatesConsumer = statesConsumer;

        final MySQLColumnMeta[] columnMetaArray = Objects.requireNonNull(
                this.prepareColumnMetas, "this.prepareColumnMetas");
        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(
                this.parameterMetas, "this.parameterMetas");

        final StatementCommandWriter commandWriter = new PrepareExecuteCommandWriter(
                this.statementId, parameterMetaArray
                , columnMetaArray.length > 0, this::addAndGetSequenceId
                , this.adjutant, this::executeCommandWriterErrorEvent);
        this.executeCommandWriter = commandWriter;

        if (parameterGroup.isEmpty()) {
            this.parameterGroupList = Collections.emptyList();
            this.batchIndex = -1;
        } else {
            this.parameterGroupList = Collections.singletonList(parameterGroup);
            this.batchIndex = 0;
        }

        updateSequenceId(-1); // reset sequence_id
        // write execute command.
        this.packetPublisher = commandWriter.writeCommand(parameterGroup);
        this.phase = Phase.READ_EXECUTE_RESPONSE;

        this.resultSetReader = new BinaryResultSetReader(sink, this.resultStatesConsumer
                , this.adjutant, this::updateSequenceId
                , this::readResultSetErrorEvent);

        // send signal to task executor.
        Objects.requireNonNull(this.signal, "this.signal").sendPacket(this);

    }


    /**
     * @see #internalError(Throwable)
     * @see #executeQueryStatement(FluxSink, List, Consumer)
     */
    private void executeCommandWriterErrorEvent(Throwable e) {
        if (this.adjutant.inEventLoop()) {
            this.errorList.add(e);
        } else {
            this.adjutant.execute(() -> this.errorList.add(e));
        }
    }

    private void readResultSetErrorEvent(Throwable e) {
        if (this.adjutant.inEventLoop()) {
            this.errorList.add(e);
        } else {
            this.adjutant.execute(() -> this.errorList.add(e));
        }
    }

    /**
     * @see #executeBatchUpdate(List)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private void executeBatchUpdateInEventLoop(final FluxSink<ResultStates> sink
            , final List<List<BindValue>> parameterGroupList) {
        switch (this.phase) {
            case CLOSE_STMT:
                sink.error(createStatementCloseException());
                break;
            case WAIT_FOR_PARAMETER: {
                if (this.querySink != null || this.updateSink != null || this.batchUpdateSink != null) {
                    throw createExecuteMultiTimeException();
                }
                executeBatchUpdateStatement(sink, parameterGroupList);
            }
            break;
            default:
                sink.error(createExecuteMultiTimeException());
        }
    }

    private void executeBatchUpdateStatement(final FluxSink<ResultStates> sink
            , final List<List<BindValue>> parameterGroupList) {

        this.phase = Phase.EXECUTE;
        this.batchUpdateSink = sink;
        this.parameterGroupList = parameterGroupList;

        this.updateSink = null;
        this.querySink = null;
        this.resultStatesConsumer = null;

        final MySQLColumnMeta[] parameterMetaArray = Objects.requireNonNull(
                this.parameterMetas, "this.parameterMetas");

        updateSequenceId(-1); // reset sequence_id
        final StatementCommandWriter commandWriter = new PrepareExecuteCommandWriter(
                this.statementId, parameterMetaArray
                , false, this::addAndGetSequenceId
                , this.adjutant, this::executeCommandWriterErrorEvent);
        this.executeCommandWriter = commandWriter;


        // write execute command.
        if (parameterGroupList.isEmpty()) {
            this.packetPublisher = commandWriter.writeCommand(Collections.emptyList());
            this.batchIndex = -1;
        } else {
            this.packetPublisher = commandWriter.writeCommand(parameterGroupList.get(0));
            this.batchIndex = 0;
        }

        this.phase = Phase.READ_EXECUTE_RESPONSE;

        // send signal to task executor.
        Objects.requireNonNull(this.signal, "this.signal").sendPacket(this);
    }


    /**
     * @return true: task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #executeQueryStatement(FluxSink, List, Consumer)
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final int header = PacketUtils.getInt1(cumulateBuffer, cumulateBuffer.readerIndex() + PacketUtils.HEADER_SIZE);
        final boolean taskEnd;
        switch (header) {
            case ErrorPacket.ERROR_HEADER: {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));

                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetResults());
                emitErrorToResultDownstream(MySQLExceptionUtils.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1(cumulateBuffer));

                OkPacket ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                Objects.requireNonNull(this.updateSink, "this.updateSink")
                        .success(MySQLResultStates.from(ok));
                taskEnd = true;
            }
            break;
            default: {
                this.phase = Phase.READ_RESULT_SET;
                taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
            }
        }
        return taskEnd;
    }

    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        return Objects.requireNonNull(this.resultSetReader, "this.resultSetReader")
                .read(cumulateBuffer, serverStatusConsumer);
    }

    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     */
    private void emitErrorToResultDownstream(Throwable e) {
        if (this.resultStatesConsumer == null) {
            Objects.requireNonNull(this.updateSink, "this.updateSink").error(e);
        } else {
            Objects.requireNonNull(this.querySink, "this.updateSink").error(e);
        }
    }



    private void markIdle() {
        this.phase = Phase.WAIT_FOR_PARAMETER;
    }


    /**
     * @see #readPrepareColumnMeta(ByteBuf, Consumer)
     * @see #readExecuteResponse(ByteBuf, Consumer)
     */
    private void resetColumnMeta(final int columnCount) {
        this.prepareColumnMetas = new MySQLColumnMeta[columnCount];
        this.columnMetaIndex = 0;
    }

    /**
     * @see #readPrepareResponse(ByteBuf)
     */
    private void resetParameterMetas(int parameterCount) {
        this.parameterMetas = new MySQLColumnMeta[parameterCount];
        this.parameterMetaIndex = 0;
    }

    /*################################## blow private instance exception method ##################################*/


    /**
     * @see #executeQueryInEventLoop(FluxSink, List, Consumer)
     */
    private IllegalStateException createStatementCloseException() {
        throw new IllegalStateException(
                String.format("%s have closed.", PreparedStatement.class.getName()));
    }

    /**
     * @see #executeQueryInEventLoop(FluxSink, List, Consumer)
     */
    private IllegalStateException createExecuteMultiTimeException() {
        throw new IllegalStateException(
                String.format("%s execute only once.", PreparedStatement.class.getName()));
    }





    /*################################## blow private static method ##################################*/




    /*################################## blow private instance inner class ##################################*/



    /*################################## blow private method ##################################*/

    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", adjutant));
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
        READ_RESULT_SET,

        RESET_STMT,
        FETCH_RESULT,
        READ_FETCH_RESULT,
        WAIT_FOR_PARAMETER,
        CLOSE_STMT
    }


}
