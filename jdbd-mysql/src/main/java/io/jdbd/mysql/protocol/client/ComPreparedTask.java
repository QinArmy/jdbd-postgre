package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.stmt.MySQLParamValue;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.result.ResultRowSink;
import io.jdbd.vendor.stmt.BatchWrapper;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.PrepareWrapper;
import io.jdbd.vendor.task.MorePacketSignal;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * <p>  code navigation :
 *     <ol>
 *         <li>decode entrance method : {@link #internalDecode(ByteBuf, Consumer)}</li>
 *         <li>send COM_STMT_PREPARE : {@link #internalStart(MorePacketSignal)} </li>
 *         <li>read COM_STMT_PREPARE Response : {@link #readPrepareResponse(ByteBuf)}
 *              <ol>
 *                  <li>read parameter meta : {@link #readPrepareParameterMeta(ByteBuf, Consumer)}</li>
 *                  <li>read prepare column meta : {@link #readPrepareColumnMeta(ByteBuf, Consumer)}</li>
 *              </ol>
 *         </li>
 *         <li>send COM_STMT_EXECUTE :
 *              <ul>
 *                  <li>{@link #executeStatement()}</li>
 *                  <li> {@link PrepareExecuteCommandWriter#writeCommand(int, List)}</li>
 *                  <li>send COM_STMT_SEND_LONG_DATA:{@link PrepareLongParameterWriter#write(int, List)}</li>
 *              </ul>
 *         </li>
 *         <li>read COM_STMT_EXECUTE Response : {@link #readExecuteResponse(ByteBuf, Consumer)}</li>
 *         <li>read Binary Protocol ResultSet Row : {@link BinaryResultSetReader#read(ByteBuf, Consumer)}</li>
 *         <li>send COM_STMT_FETCH:{@link #createFetchPacket()}</li>
 *         <li>read COM_STMT_FETCH response:{@link #readFetchResponse(ByteBuf)}</li>
 *         <li>send COM_STMT_RESET:{@link #createResetPacket()}</li>
 *         <li>read COM_STMT_RESET response:{@link #readResetResponse(ByteBuf, Consumer)}</li>
 *         <li>send COM_STMT_CLOSE : {@link #createCloseStatementPacket()}</li>
 *     </ol>
 * </p>
 *
 * <p>
 * below is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see PrepareExecuteCommandWriter
 * @see BinaryResultSetReader
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html">Prepared Statements</a>
 */
final class ComPreparedTask extends MySQLPrepareCommandTask implements StatementTask {


    /**
     * @see #ComPreparedTask(PrepareWrapper, MonoSink, MySQLTaskAdjutant)
     */
    static Mono<ResultStates> update(final PrepareWrapper wrapper, final MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(wrapper, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * @see #ComPreparedTask(PrepareWrapper, FluxSink, MySQLTaskAdjutant)
     */
    static Flux<ResultRow> query(final PrepareWrapper wrapper, final MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(wrapper, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * @see #ComPreparedTask(FluxSink, BatchWrapper, MySQLTaskAdjutant)
     */
    static <T extends MySQLParamValue> Flux<ResultStates> batchUpdate(final BatchWrapper<T> wrapper
            , final MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(sink, wrapper, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    static Mono<PreparedStatement> prepare(final String sql, final MySQLTaskAdjutant adjutant) {
        return Mono.empty();
    }

    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTask.class);

    private final DownstreamSink downstreamSink;

    private final int fetchSize;

    private StatementCommandWriter commandWriter;

    private int statementId;

    private Phase phase = Phase.PREPARED;

    private MySQLColumnMeta[] parameterMetas;

    private int parameterMetaIndex = -1;

    private MySQLColumnMeta[] prepareColumnMetas;

    private int columnMetaIndex = -1;

    private List<JdbdException> errorList;


    /**
     * @see #update(PrepareWrapper, MySQLTaskAdjutant)
     */
    private ComPreparedTask(final PrepareWrapper wrapper, final MonoSink<ResultStates> sink
            , final MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);

        this.packetPublisher = createPrepareCommand(wrapper.getSql());
        this.fetchSize = -1;
        this.downstreamSink = new UpdateDownstreamSink(wrapper, sink);
    }

    /**
     * <p>
     * create a prepare statement for  query.
     * </p>
     *
     * @see #query(PrepareWrapper, MySQLTaskAdjutant)
     */
    private ComPreparedTask(final PrepareWrapper wrapper, final FluxSink<ResultRow> sink
            , final MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.packetPublisher = createPrepareCommand(wrapper.getSql());
        if (this.properties.getOrDefault(PropertyKey.useCursorFetch, Boolean.class)) {
            // we only create cursor-backed result sets if
            // a) The query is a SELECT
            // b) The server supports it
            // c) We know it is forward-only (note this doesn't preclude updatable result sets)
            // d) The user has set a fetch size
            this.fetchSize = wrapper.getFetchSize();
        } else {
            this.fetchSize = -1;
        }
        this.downstreamSink = new QueryDownstreamSink(wrapper, sink);
    }

    /**
     * @see #batchUpdate(BatchWrapper, MySQLTaskAdjutant)
     */
    private ComPreparedTask(final FluxSink<ResultStates> sink, final BatchWrapper<? extends ParamValue> wrapper
            , final MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.packetPublisher = createPrepareCommand(wrapper.getSql());
        this.fetchSize = -1;
        this.downstreamSink = new BatchUpdateSink<>(wrapper.getParamGroupList(), sink);

    }


    @Override
    public int obtainStatementId() {
        return this.statementId;
    }

    @Override
    public MySQLColumnMeta[] obtainParameterMetas() {
        return Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
    }

    @Override
    public ClientProtocolAdjutant obtainAdjutant() {
        return this.adjutant;
    }


    @Override
    public boolean isFetchResult() {
        return this.fetchSize > 0;
    }


    /*################################## blow protected  method ##################################*/

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html">Protocol::COM_STMT_PREPARE</a>
     */
    @Override
    protected Publisher<ByteBuf> internalStart(MorePacketSignal signal) {
        final Publisher<ByteBuf> publisher = Objects.requireNonNull(this.packetPublisher, "(this.packetPublisher");
        this.packetPublisher = null;
        this.phase = Phase.READ_PREPARE_RESPONSE;
        return publisher;
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
                    if (readPrepareResponse(cumulateBuffer)) {
                        taskEnd = true;
                        continueDecode = false;

                    } else {
                        this.phase = Phase.READ_PREPARE_PARAM_META;
                        continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                    }
                }
                break;
                case READ_PREPARE_PARAM_META: {
                    if (readPrepareParameterMeta(cumulateBuffer, serverStatusConsumer)) {
                        MySQLColumnMeta[] columnMetas = this.prepareColumnMetas;
                        if (columnMetas != null && columnMetas.length > 0) {
                            this.phase = Phase.READ_PREPARE_COLUMN_META;
                            continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                        } else {
                            this.phase = Phase.EXECUTE;
                            taskEnd = executeStatement(); // execute command
                            if (!taskEnd) {
                                this.phase = Phase.READ_EXECUTE_RESPONSE;
                            }
                            continueDecode = false;
                        }
                    } else {
                        continueDecode = false;
                    }
                }
                break;
                case READ_PREPARE_COLUMN_META: {
                    if (readPrepareColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                        this.phase = Phase.EXECUTE;
                        taskEnd = executeStatement(); // execute command
                        if (!taskEnd) {
                            this.phase = Phase.READ_EXECUTE_RESPONSE;
                        }
                    }
                    continueDecode = false;
                }
                break;
                case READ_EXECUTE_RESPONSE: {
                    // maybe modify this.phase
                    taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                    continueDecode = false;
                }
                break;
                case READ_RESULT_SET: {
                    taskEnd = readResultSet(cumulateBuffer, serverStatusConsumer);
                    continueDecode = false;
                }
                break;
                case READ_RESET_RESPONSE: {
                    if (readResetResponse(cumulateBuffer, serverStatusConsumer)) {
                        taskEnd = true;
                        continueDecode = false;
                    } else {
                        this.phase = Phase.EXECUTE;
                        taskEnd = executeStatement(); // execute command
                        if (!taskEnd) {
                            this.phase = Phase.READ_EXECUTE_RESPONSE;
                        }
                    }
                }
                break;
                case READ_FETCH_RESPONSE: {
                    if (readFetchResponse(cumulateBuffer)) {
                        taskEnd = true;
                        continueDecode = false;
                    } else {
                        this.phase = Phase.READ_RESULT_SET;
                        continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                    }
                }
                break;
                default:
                    throw new IllegalStateException(String.format("this.phase[%s] error.", this.phase));
            }
        }
        if (taskEnd) {
            this.packetPublisher = Mono.just(createCloseStatementPacket());
            this.phase = Phase.CLOSE_STMT;
            if (hasError()) {
                this.downstreamSink.error(createException());
            } else {
                this.downstreamSink.complete();
            }
        }
        return taskEnd;
    }


    /**
     * @see #error(Throwable)
     */
    @Override
    protected Action internalError(Throwable e) {
        final Action action;
        switch (this.phase) {
            case PREPARED:
                action = Action.TASK_END;
                break;
            case CLOSE_STMT: {
                throw new IllegalStateException("CLOSE_STM command send error.");
            }
            default: {
                addError(MySQLExceptions.wrap(e));
                this.downstreamSink.error(createException());
                this.packetPublisher = Mono.just(createCloseStatementPacket());
                action = Action.MORE_SEND_PACKET;
            }

        }
        return action;
    }

    @Override
    protected void internalOnChannelClose() {
        if (this.phase != Phase.CLOSE_STMT) {
            this.downstreamSink.error(new SessionCloseException("Database session have closed."));
        }
    }

    /*################################## blow private method ##################################*/

    private Publisher<ByteBuf> createPrepareCommand(String sql) throws SQLException, JdbdSQLException {
        assertPhase(Phase.PREPARED);
        return PacketUtils.createSimpleCommand(PacketUtils.COM_STMT_PREPARE, sql
                , this.adjutant, this::addAndGetSequenceId);
    }


    private boolean hasError() {
        return !MySQLCollections.isEmpty(this.errorList);
    }

    private void addError(JdbdException e) {
        List<JdbdException> errorList = this.errorList;
        if (errorList == null) {
            errorList = new ArrayList<>();
            this.errorList = errorList;
        }
        errorList.add(e);
    }


    /**
     * @return true: prepare error,task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response">COM_STMT_PREPARE Response</a>
     */
    private boolean readPrepareResponse(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_PREPARE_RESPONSE);

        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));
        final int headFlag = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex()); //1. status/error header
        final boolean taskEnd;
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                addError(MySQLExceptions.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case 0: {
                final int payloadStartIndex = cumulateBuffer.readerIndex();
                cumulateBuffer.skipBytes(1);//skip status
                this.statementId = PacketUtils.readInt4(cumulateBuffer);//2. statement_id
                resetColumnMeta(PacketUtils.readInt2AsInt(cumulateBuffer));//3. num_columns
                resetParameterMetas(PacketUtils.readInt2AsInt(cumulateBuffer));//4. num_params
                cumulateBuffer.skipBytes(1); //5. skip filler
                PacketUtils.readInt2AsInt(cumulateBuffer);//6. warning_count
                if ((this.negotiatedCapability & ClientProtocol.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
                    throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA"); //7. metadata_follows
                }
                cumulateBuffer.readerIndex(payloadStartIndex + payloadLength); // to next packet,avoid tail filler.
                taskEnd = false;
            }
            break;
            default: {
                throw MySQLExceptions.createFatalIoException(
                        "Server send COM_STMT_PREPARE Response error. header[%s]", headFlag);
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
    private boolean readPrepareParameterMeta(final ByteBuf cumulateBuffer
            , final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_PREPARE_PARAM_META);
        int parameterMetaIndex = this.parameterMetaIndex;
        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
        if (parameterMetaIndex < metaArray.length) {
            parameterMetaIndex = BinaryResultSetReader.readColumnMeta(cumulateBuffer, metaArray
                    , parameterMetaIndex, this::updateSequenceId, this.adjutant);
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
        assertPhase(Phase.READ_PREPARE_COLUMN_META);

        final MySQLColumnMeta[] metaArray = Objects.requireNonNull(this.prepareColumnMetas, "this.prepareColumnMetas");
        int columnMetaIndex = this.columnMetaIndex;
        if (columnMetaIndex < metaArray.length) {
            columnMetaIndex = BinaryResultSetReader.readColumnMeta(cumulateBuffer, metaArray
                    , columnMetaIndex, this::updateSequenceId, this.adjutant);
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
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));
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
     * @see #internalError(Throwable)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html">Protocol::COM_STMT_CLOSE</a>
     */
    private ByteBuf createCloseStatementPacket() {
        ByteBuf packet = this.adjutant.allocator().buffer(9);

        PacketUtils.writeInt3(packet, 5);
        packet.writeByte(0);// use 0 sequence_id

        packet.writeByte(PacketUtils.COM_STMT_CLOSE);
        PacketUtils.writeInt4(packet, this.statementId);
        return packet;
    }


    /**
     * <p>
     * modify {@link #phase}
     * </p>
     *
     * @return true: task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see #executeStatement()
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final int header = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() + PacketUtils.HEADER_SIZE);
        final boolean taskEnd;
        switch (header) {
            case ErrorPacket.ERROR_HEADER: {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));

                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                addError(MySQLExceptions.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));

                OkPacket ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                // emit update result
                if (this.downstreamSink instanceof UpdateDownstreamSink) {
                    ((UpdateDownstreamSink) this.downstreamSink).success(MySQLResultStates.from(ok));
                    taskEnd = true;
                } else {
                    BatchUpdateSink<?> batchUpdateSink = ((BatchUpdateSink<?>) this.downstreamSink);
                    if (batchUpdateSink.next(MySQLResultStates.from(ok))) {
                        this.phase = Phase.RESET_STMT;
                        this.packetPublisher = Mono.just(createResetPacket());
                        this.phase = Phase.READ_RESET_RESPONSE;
                        taskEnd = false;
                    } else {
                        taskEnd = true;
                    }
                }
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
     * <p>
     * modify {@link #phase}
     * </p>
     *
     * @return true: task end.
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_RESULT_SET);
        final QueryDownstreamSink downstreamSink = (QueryDownstreamSink) this.downstreamSink;
        boolean taskEnd = false;
        if (downstreamSink.readResultSet(cumulateBuffer, serverStatusConsumer)) {
            if (downstreamSink.hasMoreResults()) {
                this.phase = Phase.READ_RESULT_SET;
            } else if (downstreamSink.hasMoreFetch()) {
                this.phase = Phase.FETCH_STMT;
                this.packetPublisher = Mono.just(createFetchPacket());
                this.phase = Phase.READ_FETCH_RESPONSE;
            } else {
                taskEnd = true;
            }
        }
        return taskEnd;
    }

    /**
     * <p>
     * modify {@link #phase}
     * </p>
     *
     * @return true: task end.
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private boolean readFetchResponse(final ByteBuf cumulateBuffer) {
        final int flag = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() + PacketUtils.HEADER_SIZE);
        boolean taskEnd = false;
        if (flag == ErrorPacket.ERROR_HEADER) {
            final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
            updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));
            ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                    , this.negotiatedCapability, this.adjutant.obtainCharsetError());
            addError(MySQLExceptions.createErrorPacketException(error));
            taskEnd = true;
        }
        return taskEnd;
    }

    /**
     * @return true : reset occur error,task end.
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html">Protocol::COM_STMT_RESET</a>
     */
    private boolean readResetResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
        updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));

        final int flag = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex());
        final boolean taskEnd;
        switch (flag) {
            case ErrorPacket.ERROR_HEADER: {
                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                addError(MySQLExceptions.createErrorPacketException(error));
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                OkPacket ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(MySQLResultStates.from(ok));
                taskEnd = false;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_STMT_RESET response error,flag[%s].", flag);
        }
        return taskEnd;
    }


    /**
     * @return true : task end:<ul>
     * <li>bind parameter error</li>
     * <li>batch update end</li>
     * </ul>
     * @see #internalDecode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private boolean executeStatement() {
        assertPhase(Phase.EXECUTE);
        updateSequenceId(-1); // reset sequenceId

        final DownstreamSink downStreamSink = this.downstreamSink;

        final MySQLColumnMeta[] columnMetaArray = Objects.requireNonNull(
                this.prepareColumnMetas, "this.prepareColumnMetas");

        boolean taskEnd = false;
        try {
            if (downStreamSink instanceof QueryDownstreamSink) {
                if (columnMetaArray.length == 0) {
                    addError(new ErrorSubscribeException(ResultType.QUERY, ResultType.UPDATE));
                    taskEnd = true;
                } else {
                    List<? extends ParamValue> parameterGroup = ((QueryDownstreamSink) downStreamSink).parameterGroup;
                    this.packetPublisher = new PrepareExecuteCommandWriter(this).writeCommand(-1, parameterGroup);
                }
            } else if (downStreamSink instanceof UpdateDownstreamSink) {
                if (columnMetaArray.length > 0) {
                    addError(new ErrorSubscribeException(ResultType.UPDATE, ResultType.QUERY));
                    taskEnd = true;
                } else {
                    List<? extends ParamValue> parameterGroup = ((UpdateDownstreamSink) downStreamSink).parameterGroup;
                    this.packetPublisher = new PrepareExecuteCommandWriter(this).writeCommand(-1, parameterGroup);
                }
            } else if (downStreamSink instanceof BatchUpdateSink) {
                if (columnMetaArray.length > 0) {
                    addError(new ErrorSubscribeException(ResultType.BATCH_UPDATE, ResultType.QUERY));
                    taskEnd = true;
                } else {
                    taskEnd = executeBatchUpdateStatement((BatchUpdateSink<?>) downStreamSink);
                }
            } else {
                throw new IllegalStateException(String.format("Unknown DownstreamSink[%s]", downStreamSink));
            }

        } catch (Throwable e) {
            addError(MySQLExceptions.wrap(e));
            taskEnd = true;
        }
        return taskEnd;
    }


    /**
     * @return true:task end.
     * @see #executeStatement()
     */
    private <T extends ParamValue> boolean executeBatchUpdateStatement(final BatchUpdateSink<T> batchSink)
            throws JdbdSQLException {

        List<List<T>> groupList = batchSink.groupList;
        final List<T> parameterGroup;
        final int index = batchSink.index++;

        final boolean taskEnd;
        if (index < groupList.size()) {
            StatementCommandWriter commandWriter = this.commandWriter;
            if (commandWriter == null) {
                commandWriter = new PrepareExecuteCommandWriter(this);
                this.commandWriter = commandWriter;
            }
            parameterGroup = groupList.get(index);
            try {
                this.packetPublisher = commandWriter.writeCommand(index, parameterGroup); // write command
            } catch (SQLException e) {
                throw new JdbdSQLException(e, "Batch update[batchIndex:%s] write error.", index);
            }
            taskEnd = false;
        } else {
            taskEnd = true;
        }
        return taskEnd;
    }

    /**
     * @see #readResultSet(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_fetch.html">Protocol::COM_STMT_FETCH</a>
     */
    private ByteBuf createFetchPacket() {
        assertPhase(Phase.FETCH_STMT);
        final DownstreamSink downStreamSink = this.downstreamSink;
        if (!(downStreamSink instanceof QueryDownstreamSink)) {
            throw new IllegalStateException(String.format("downStreamSink[%s] isn't QuerySink", downStreamSink));
        }
        if (this.fetchSize < 1) {
            throw new IllegalStateException("Not fetch mode ,reject execute fetch.");
        }
        final QueryDownstreamSink querySink = (QueryDownstreamSink) downStreamSink;

        if (!querySink.hasMoreFetch()) {
            throw new IllegalStateException("Fetch mode have sent last row.");
        }
        ByteBuf packet = this.adjutant.allocator().buffer(13);
        PacketUtils.writeInt3(packet, 9);
        packet.writeByte(addAndGetSequenceId());

        packet.writeByte(PacketUtils.COM_STMT_FETCH);
        PacketUtils.writeInt4(packet, this.statementId);
        PacketUtils.writeInt4(packet, this.fetchSize);

        return packet;
    }

    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html">Protocol::COM_STMT_RESET</a>
     */
    private ByteBuf createResetPacket() {
        assertPhase(Phase.RESET_STMT);

        final DownstreamSink downStreamSink = this.downstreamSink;
        if (!(downStreamSink instanceof BatchUpdateSink)) {
            throw new IllegalStateException(String.format(
                    "this.downstreamSink[%s] isn't %s,reject COM_STMT_RESET command."
                    , this.downstreamSink, BatchUpdateSink.class.getSimpleName()));
        }
        if (!((BatchUpdateSink<?>) downStreamSink).hasMoreGroup()) {
            throw new IllegalStateException("Batch update have ended");
        }
        ByteBuf packet = this.adjutant.allocator().buffer(9);

        PacketUtils.writeInt3(packet, 5);
        packet.writeByte(addAndGetSequenceId());

        packet.writeByte(PacketUtils.COM_STMT_RESET);
        PacketUtils.writeInt4(packet, this.statementId);
        return packet;
    }


    /**
     * @see #readPrepareColumnMeta(ByteBuf, Consumer)
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see #readPrepareResponse(ByteBuf)
     */
    private void resetColumnMeta(final int columnCount) {
        if (columnCount == 0) {
            this.prepareColumnMetas = MySQLColumnMeta.EMPTY;
        } else {
            this.prepareColumnMetas = new MySQLColumnMeta[columnCount];
        }
        this.columnMetaIndex = 0;
    }

    /**
     * @see #readPrepareParameterMeta(ByteBuf, Consumer) (ByteBuf)
     * @see #readPrepareResponse(ByteBuf)
     */
    private void resetParameterMetas(final int parameterCount) {
        if (parameterCount == 0) {
            this.parameterMetas = MySQLColumnMeta.EMPTY;
        } else {
            this.parameterMetas = new MySQLColumnMeta[parameterCount];
        }
        this.parameterMetaIndex = 0;
    }


    /**
     * @see #internalDecode(ByteBuf, Consumer)
     */
    private JdbdException createException() {
        final List<JdbdException> errorList = this.errorList;
        if (errorList == null || errorList.isEmpty()) {
            throw new IllegalStateException("No error.");
        }
        JdbdException e;
        if (errorList.size() == 1) {
            e = errorList.get(0);
        } else {
            e = new JdbdCompositeException(errorList
                    , "MultiResults read occur multi error,the first error[%s]", errorList.get(0).getMessage());
        }
        return e;
    }


    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", expectedPhase));
        }
    }

    /*################################## blow private static method ##################################*/



    /*################################## blow private static inner class ##################################*/


    private interface DownstreamSink {

        void error(JdbdException e);

        void complete();
    }

    private final class QueryDownstreamSink implements DownstreamSink, ResultRowSink {

        private final ResultSetReader resultSetReader;

        private final List<? extends ParamValue> parameterGroup;

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultStates> statesConsumer;

        private ResultStates resultStates;

        private QueryDownstreamSink(PrepareWrapper wrapper, FluxSink<ResultRow> sink) {

            this.resultSetReader = ResultSetReaderBuilder
                    .builder()

                    .rowSink(this)
                    .adjutant(ComPreparedTask.this.adjutant)
                    .fetchResult(ComPreparedTask.this.fetchSize > 0)
                    .sequenceIdUpdater(ComPreparedTask.this::updateSequenceId)

                    .errorConsumer(ComPreparedTask.this::addError)
                    .build(BinaryResultSetReader.class);

            this.parameterGroup = wrapper.getParamGroup();
            this.sink = sink;
            this.statesConsumer = wrapper.getStatesConsumer();
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public void complete() {
            ResultStates resultStates = Objects.requireNonNull(this.resultStates, "this.resultStates");
            try {
                this.statesConsumer.accept(resultStates);
                this.sink.complete();
            } catch (Throwable e) {
                this.sink.error(new ResultStateConsumerException(e, "%s consumer occur error."
                        , ResultStates.class.getName()));
            }
        }


        @Override
        public void next(ResultRow resultRow) {
            if (!ComPreparedTask.this.hasError()) {
                this.sink.next(resultRow);
            }
        }

        @Override
        public boolean isCancelled() {
            return this.sink.isCancelled();
        }

        @Override
        public void accept(final ResultStates resultStates) {
            this.resultStates = resultStates;
        }


        public boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            final ResultStates lastResultStates = this.resultStates;
            resultSetEnd = this.resultSetReader.read(cumulateBuffer, serverStatusConsumer);
            if (resultSetEnd) {
                ResultStates resultStates = this.resultStates;
                if (resultStates == null || resultStates == lastResultStates) {
                    throw new IllegalStateException(String.format("%s not invoke %s.accept(ResultStates) method."
                            , this.resultSetReader, ResultRowSink.class.getName()));
                }
            }
            return resultSetEnd;
        }

        public boolean hasMoreResults() {
            final int serverStatus = ((MySQLResultStates) this.resultStates).getServerStatus();
            return (serverStatus & ClientProtocol.SERVER_MORE_RESULTS_EXISTS) != 0;
        }

        public boolean hasMoreFetch() {
            final int serverStatus = ((MySQLResultStates) this.resultStates).getServerStatus();
            return (serverStatus & ClientProtocol.SERVER_STATUS_CURSOR_EXISTS) != 0
                    && (serverStatus & ClientProtocol.SERVER_STATUS_LAST_ROW_SENT) == 0;
        }

    }

    private static final class UpdateDownstreamSink implements DownstreamSink {

        private final List<? extends ParamValue> parameterGroup;

        private final MonoSink<ResultStates> sink;

        private ResultStates resultStates;

        /**
         * @see #ComPreparedTask(PrepareWrapper, MonoSink, MySQLTaskAdjutant)
         */
        private UpdateDownstreamSink(PrepareWrapper wrapper, MonoSink<ResultStates> sink) {
            this.parameterGroup = wrapper.getParamGroup();
            this.sink = sink;
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        public void success(final ResultStates resultStates) {
            this.resultStates = resultStates;
        }

        @Override
        public void complete() {
            this.sink.success(Objects.requireNonNull(this.resultStates, "this.resultStates"));
        }
    }

    private static final class BatchUpdateSink<T extends ParamValue> implements DownstreamSink {

        private final List<List<T>> groupList;

        private int index = 0;

        private final FluxSink<ResultStates> sink;

        /**
         * @see #ComPreparedTask(FluxSink, BatchWrapper, MySQLTaskAdjutant)
         */
        private BatchUpdateSink(List<List<T>> groupList, FluxSink<ResultStates> sink) {
            this.groupList = groupList;
            this.sink = sink;
        }

        @Override
        public void error(JdbdException e) {
            this.sink.error(e);
        }

        /**
         * @return true ,has more,reset statement.
         */
        public boolean next(final ResultStates resultStates) {
            this.sink.next(resultStates);
            return hasMoreGroup();
        }

        public boolean hasMoreGroup() {
            return this.index < this.groupList.size();
        }

        @Override
        public void complete() {
            this.sink.complete();
        }
    }


    enum Phase {
        PREPARED,
        READ_PREPARE_RESPONSE,
        READ_PREPARE_PARAM_META,
        READ_PREPARE_COLUMN_META,

        EXECUTE,
        READ_EXECUTE_RESPONSE,
        READ_RESULT_SET,

        RESET_STMT,
        READ_RESET_RESPONSE,

        FETCH_STMT,
        READ_FETCH_RESPONSE,

        CLOSE_STMT
    }


}
