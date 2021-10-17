package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.SessionCloseException;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.JdbdWarning;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * below is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see PrepareExecuteCommandWriter
 * @see PrepareLongParameterWriter
 * @see BinaryResultSetReader
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html">Prepared Statements</a>
 */
final class ComPreparedTask extends AbstractCommandTask implements PrepareStmtTask, PrepareTask<MySQLType> {


    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method:
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#bindUpdate(BindStmt, TaskAdjutant)
     */
    static Mono<ResultStates> update(final ParamStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *      <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#bindQuery(BindStmt, TaskAdjutant)
     */
    static Flux<ResultRow> query(final ParamStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#bindBatch(BindBatchStmt, TaskAdjutant)
     */
    static Flux<ResultStates> batchUpdate(final ParamBatchStmt<? extends ParamValue> stmt
            , final TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }

        });
    }


    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsMulti()} method.
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#bindBatchAsMulti(BindBatchStmt, TaskAdjutant)
     */
    static MultiResult batchAsMulti(final ParamBatchStmt<? extends ParamValue> stmt, final TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindStatement#executeBatchAsFlux()} method.
     * </p>
     *
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     * @see ComQueryTask#bindBatchAsFlux(BindBatchStmt, TaskAdjutant)
     */
    static OrderedFlux batchAsFlux(final ParamBatchStmt<? extends ParamValue> stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link DatabaseSession#prepare(String)}</li>
     * </ul>
     * </p>
     *
     * @see DatabaseSession#prepare(String)
     * @see #ComPreparedTask(ParamSingleStmt, ResultSink, TaskAdjutant)
     */
    static Mono<PreparedStatement> prepare(final String sql, final TaskAdjutant adjutant
            , final Function<PrepareTask<MySQLType>, PreparedStatement> function) {
        return Mono.create(sink -> {
            try {
                final MySQLPrepareStmt stmt = new MySQLPrepareStmt(sql);
                final ResultSink resultSink = new PrepareSink(sink, function);
                ComPreparedTask task = new ComPreparedTask(stmt, resultSink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTask.class);

    private final ParamSingleStmt stmt;

    private final ExecuteCommandWriter commandWriter;

    private int statementId;

    private Warning warning;

    private Phase phase = Phase.PREPARED;

    private MySQLColumnMeta[] paramMetas;

    private ResultRowMeta rowMeta;

    /**
     * @see #nextGroupReset()
     * @see #readResetResponse(ByteBuf, Consumer)
     */
    private boolean nextGroupNeedReset;

    private int batchIndex = 0;

    /**
     * @see #update(ParamStmt, TaskAdjutant)
     */
    private ComPreparedTask(final ParamSingleStmt stmt, final ResultSink sink, final TaskAdjutant adjutant) {
        super(adjutant, sink);
        this.stmt = stmt;
        this.commandWriter = PrepareExecuteCommandWriter.create(this);
    }


    @Override
    public int obtainStatementId() {
        if (this.paramMetas == null) {
            throw new IllegalStateException("before prepare");
        }
        return this.statementId;
    }

    @Override
    public MySQLColumnMeta[] obtainParameterMetas() {
        return Objects.requireNonNull(this.paramMetas, "this.parameterMetas");
    }

    @Override
    public TaskAdjutant obtainAdjutant() {
        return this.adjutant;
    }


    @Override
    public boolean supportFetch() {
        return false;
    }

    @Override
    public void nextGroupReset() {
        this.nextGroupNeedReset = true;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    /*################################## blow PrepareStmtTask method ##################################*/

    /**
     * @see PrepareTask#executeUpdate(ParamStmt)
     */
    @Override
    public Mono<ResultStates> executeUpdate(final ParamStmt stmt) {
        return MultiResults.update(sink -> executeAfterBinding(sink, stmt));
    }

    /**
     * @see PrepareTask#executeQuery(ParamStmt)
     */
    @Override
    public Flux<ResultRow> executeQuery(ParamStmt stmt) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> executeAfterBinding(sink, stmt));
    }


    /**
     * @see PrepareTask#executeBatch(ParamBatchStmt)
     */
    @Override
    public Flux<ResultStates> executeBatch(final ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.batchUpdate(sink -> executeAfterBinding(sink, stmt));
    }

    /**
     * @see PrepareTask#executeBatchAsMulti(ParamBatchStmt)
     */
    @Override
    public MultiResult executeBatchAsMulti(ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.asMulti(this.adjutant, sink -> executeAfterBinding(sink, stmt));
    }

    /**
     * @see PrepareTask#executeBatchAsFlux(ParamBatchStmt)
     */
    @Override
    public OrderedFlux executeBatchAsFlux(ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.asFlux(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public List<MySQLType> getParamTypes() {
        final MySQLColumnMeta[] paramMetas = this.paramMetas;
        if (paramMetas == null) {
            throw new IllegalStateException("this.paramMetas is null");
        }
        final List<MySQLType> paramTypeList;
        switch (paramMetas.length) {
            case 0: {
                paramTypeList = Collections.emptyList();
            }
            break;
            case 1: {
                paramTypeList = Collections.singletonList(paramMetas[0].sqlType);
            }
            break;
            default: {
                final List<MySQLType> list = new ArrayList<>(paramMetas.length);
                for (MySQLColumnMeta paramMeta : paramMetas) {
                    list.add(paramMeta.sqlType);
                }
                paramTypeList = Collections.unmodifiableList(list);
            }
        }
        return paramTypeList;
    }

    @Nullable
    @Override
    public ResultRowMeta getRowMeta() {
        return this.rowMeta;
    }

    @Override
    public void closeOnBindError(final Throwable error) {
        if (this.adjutant.inEventLoop()) {
            closeOnBindErrorInEventLoop(error);
        } else {
            this.adjutant.execute(() -> closeOnBindErrorInEventLoop(error));
        }
    }

    @Override
    public String getSql() {
        return this.stmt.getSql();
    }

    @Override
    public Mono<Void> abandonBind() {
        return Mono.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                abandonBindInEventLoop(sink);
            } else {
                this.adjutant.execute(() -> abandonBindInEventLoop(sink));
            }
        });
    }

    @Nullable
    @Override
    public Warning getWarning() {
        return this.warning;
    }

    /*################################## blow CommunicationTask protected  method ##################################*/

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html">Protocol::COM_STMT_PREPARE</a>
     */
    @Override
    protected Publisher<ByteBuf> start() {
        Publisher<ByteBuf> publisher;
        try {
            publisher = Packets.createSimpleCommand(Packets.COM_STMT_PREPARE, this.stmt.getSql()
                    , this.adjutant, this::addAndGetSequenceId);

            this.phase = Phase.READ_PREPARE_RESPONSE;

            if (LOG.isTraceEnabled()) {
                LOG.trace(" {} prepare with stmt[{}]", this, this.stmt.getClass().getSimpleName());
            }
        } catch (Throwable e) {
            publisher = null;
            this.phase = Phase.ERROR_ON_START;
            addError(e);
        }
        return publisher;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (this.phase == Phase.ERROR_ON_START) {
            publishError(this.sink::error);
            return true;
        }
        boolean taskEnd = false, continueDecode = Packets.hasOnePacket(cumulateBuffer);
        while (continueDecode) {
            switch (this.phase) {
                case READ_PREPARE_RESPONSE: {
                    if (canReadPrepareResponse(cumulateBuffer)) {
                        // possibly PreparedStatement bind occur error
                        taskEnd = readPrepareResponse(cumulateBuffer)
                                || this.phase == Phase.ERROR_ON_BINDING
                                || this.phase == Phase.ABANDON_BINDING;
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
                    } else {
                        this.phase = Phase.EXECUTE;
                        taskEnd = executeNextGroup(); // execute command
                    }
                    continueDecode = false;
                }
                break;
                case READ_FETCH_RESPONSE: {
                    if (readFetchResponse(cumulateBuffer)) {
                        taskEnd = true;
                        continueDecode = false;
                    } else {
                        this.phase = Phase.READ_RESULT_SET;
                        continueDecode = Packets.hasOnePacket(cumulateBuffer);
                    }
                }
                break;
                default:
                    throw MySQLExceptions.createUnexpectedEnumException(this.phase);
            }
        }
        if (taskEnd) {
            if (this.phase != Phase.READ_PREPARE_RESPONSE) {
                this.phase = Phase.END;
                this.packetPublisher = Mono.just(createCloseStatementPacket());
            }
            if (hasError()) {
                publishError(this.sink::error);
            } else {
                this.sink.complete();
            }
        }
        return taskEnd;
    }


    @Override
    protected Action onError(Throwable e) {
        final Action action;
        switch (this.phase) {
            case PREPARED:
                action = Action.TASK_END;
                break;
            case END: {
                throw new IllegalStateException("CLOSE_STMT command send error.", e);
            }
            default: {
                addError(MySQLExceptions.wrap(e));
                this.packetPublisher = Mono.just(createCloseStatementPacket());
                action = Action.MORE_SEND_AND_END;
            }

        }
        return action;
    }

    @Override
    protected void onChannelClose() {
        if (this.phase != Phase.END) {
            this.sink.error(new SessionCloseException("Database session unexpected close."));
        }
    }

    /*################################## blow packet template method ##################################*/

    @Override
    void handleReadResultSetEnd() {
        this.phase = Phase.READ_EXECUTE_RESPONSE;
    }

    @Override
    ResultSetReader createResultSetReader() {
        return BinaryResultSetReader.create(this);
    }

    @Override
    boolean hasMoreGroup() {
        final ParamSingleStmt stmt = getActualStmt();
        final boolean moreGroup;
        if (stmt instanceof ParamBatchStmt) {
            moreGroup = this.batchIndex < ((ParamBatchStmt<?>) stmt).getGroupList().size();
            if (moreGroup) {
                this.phase = Phase.EXECUTE;
            }
        } else {
            moreGroup = false;
        }
        return moreGroup;
    }


    /**
     * @return true : task end.
     * @see #handleReadPrepareComplete()
     */
    boolean executeNextGroup() {
        assertPhase(Phase.EXECUTE);

        if (this.nextGroupNeedReset) {
            this.nextGroupNeedReset = false;
            this.packetPublisher = Mono.just(createResetPacket());
            this.phase = Phase.READ_RESET_RESPONSE;
            return false;
        }

        final ParamSingleStmt stmt = this.stmt;
        final ParamSingleStmt actualStmt;
        if (stmt instanceof MySQLPrepareStmt) {
            actualStmt = ((PrepareStmt) stmt).getStmt();
        } else {
            actualStmt = stmt;
        }

        final int batchIndex;
        if (actualStmt instanceof ParamStmt) {
            if (this.batchIndex++ != 0) {
                throw new IllegalStateException(String.format("%s duplication execution.", ParamStmt.class.getName()));
            }
            batchIndex = -1;
        } else {
            batchIndex = this.batchIndex++;
        }
        boolean taskEnd = false;
        try {
            this.packetPublisher = this.commandWriter.writeCommand(batchIndex);
            this.phase = Phase.READ_EXECUTE_RESPONSE;
        } catch (Throwable e) {
            taskEnd = true;
            addError(e);
        }
        return taskEnd;
    }

    /**
     * @return true: task end.
     * @see #readResultSet(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_fetch.html">Protocol::COM_STMT_FETCH</a>
     */
    @Override
    boolean executeNextFetch() {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final int fetchSize = getActualStmt().getFetchSize();
        final boolean taskEnd;
        if (fetchSize > 0 && this.rowMeta != null) {
            final ByteBuf packet = this.adjutant.allocator().buffer(13);

            Packets.writeInt3(packet, 9);
            packet.writeByte(addAndGetSequenceId());

            packet.writeByte(Packets.COM_STMT_FETCH);
            Packets.writeInt4(packet, this.statementId);
            Packets.writeInt4(packet, fetchSize);

            this.packetPublisher = Mono.just(packet);
            taskEnd = false;
        } else {
            taskEnd = true;
            // here bug or MySQL server status error.
            addError(new IllegalStateException("no more fetch."));
        }
        return taskEnd;
    }

    /*################################## blow private method ##################################*/

    private ParamSingleStmt getActualStmt() {
        ParamSingleStmt stmt = this.stmt;
        if (stmt instanceof PrepareStmt) {
            stmt = ((PrepareStmt) stmt).getStmt();
        }
        return stmt;
    }


    /**
     * @see #executeUpdate(ParamStmt)
     * @see #executeQuery(ParamStmt)
     * @see #executeBatch(ParamBatchStmt)
     * @see #executeBatchAsMulti(ParamBatchStmt)
     * @see #executeBatchAsFlux(ParamBatchStmt)
     */
    private void executeAfterBinding(final ResultSink sink, final ParamSingleStmt stmt) {
        if (this.adjutant.inEventLoop()) {
            executeAfterBindingInEventLoop(sink, stmt);
        } else {
            this.adjutant.execute(() -> executeAfterBindingInEventLoop(sink, stmt));
        }
    }

    /**
     * @see #executeAfterBinding(ResultSink, ParamSingleStmt)
     */
    private void executeAfterBindingInEventLoop(final ResultSink sink, final ParamSingleStmt stmt) {
        switch (this.phase) {
            case WAIT_FOR_BINDING: {
                try {
                    ((MySQLPrepareStmt) this.stmt).setStmt(stmt);
                    ((PrepareSink) this.sink).setSink(sink);
                    this.phase = Phase.EXECUTE;
                    if (executeNextGroup()) {
                        endTaskForErrorOnWaitForBinding();
                    } else {
                        this.phase = Phase.READ_EXECUTE_RESPONSE;
                    }
                } catch (Throwable e) {
                    // here bug
                    this.phase = Phase.ERROR_ON_BINDING;
                    endTaskForErrorOnWaitForBinding();
                    sink.error(e);
                }
            }
            break;
            case END: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            default: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is executing,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
        }

    }

    /**
     * @see #abandonBind()
     */
    private void abandonBindInEventLoop(final MonoSink<Void> sink) {
        // error can't be emitted to sink ,because not actual sink now.
        switch (this.phase) {
            case WAIT_FOR_BINDING: {
                this.phase = Phase.ABANDON_BINDING;
                endTaskForErrorOnWaitForBinding();
                sink.success();
            }
            break;
            case END: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            case ERROR_ON_BINDING:
            case ABANDON_BINDING: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("this.phase is {},but reuse {}", this.phase, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
            break;
            default: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is executing,but reuse {}", this, PreparedStatement.class.getName());
                }
                sink.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
            }
        } // switch
    }

    /**
     * @see #closeOnBindError(Throwable)
     */
    private void closeOnBindErrorInEventLoop(final Throwable error) {
        // error can't be emitted to sink ,because not actual sink now.
        switch (this.phase) {
            case WAIT_FOR_BINDING: {
                this.phase = Phase.ERROR_ON_BINDING;
                endTaskForErrorOnWaitForBinding();
            }
            break;
            case END: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} have ended,but reuse {}", this, PreparedStatement.class.getName(), error);
                }
            }
            break;
            case ERROR_ON_BINDING:
            case ABANDON_BINDING: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("this.phase is {},but reuse {}", this.phase, PreparedStatement.class.getName(), error);
                }
            }
            break;
            default: {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} is executing,but reuse {}", this, PreparedStatement.class.getName(), error);
                }
            }
        } // switch

    }

    /**
     * @see #closeOnBindErrorInEventLoop(Throwable)
     */
    private void endTaskForErrorOnWaitForBinding() {
        if (!this.inDecodeMethod()) {
            this.packetPublisher = Mono.just(createCloseStatementPacket());
            this.sendPacketSignal(true)
                    .doOnSuccess(v -> this.phase = Phase.END)
                    .subscribe();
        }

    }


    /**
     * @return false : need more cumulate
     * @see #readPrepareResponse(ByteBuf)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response">COM_STMT_PREPARE Response</a>
     */
    private boolean canReadPrepareResponse(final ByteBuf cumulateBuffer) {
        final int originalReaderIndex = cumulateBuffer.readerIndex();

        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.readByte();// skip sequenceId byte.

        final int payloadIndex = cumulateBuffer.readerIndex();
        final boolean canRead;
        final int headFlag = Packets.readInt1AsInt(cumulateBuffer);
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER: {
                canRead = true;
            }
            break;
            case 0: {
                cumulateBuffer.skipBytes(5); //skip status and statementId
                final int numColumns = Packets.readInt2AsInt(cumulateBuffer);
                final int numParams = Packets.readInt2AsInt(cumulateBuffer);
                cumulateBuffer.readerIndex(payloadIndex + payloadLength); // to next packet.

                final boolean deprecateEof = Capabilities.deprecateEof(this.negotiatedCapability);
                final int packetNumber = deprecateEof ? (numParams + numColumns) : (numParams + numColumns + 2);
                canRead = Packets.hasPacketNumber(cumulateBuffer, packetNumber);
            }
            break;
            default: {
                cumulateBuffer.readerIndex(originalReaderIndex);
                throw MySQLExceptions.createFatalIoException(
                        "Server send COM_STMT_PREPARE Response error. header[%s]", headFlag);
            }

        }
        cumulateBuffer.readerIndex(originalReaderIndex);
        return canRead;
    }


    /**
     * @return true: prepare error,task end.
     * @see #decode(ByteBuf, Consumer)
     * @see #canReadPrepareResponse(ByteBuf)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response">COM_STMT_PREPARE Response</a>
     */
    private boolean readPrepareResponse(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_PREPARE_RESPONSE);

        final int headFlag = Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() + Packets.HEADER_SIZE);
        final boolean taskEnd;
        switch (headFlag) {
            case ErrorPacket.ERROR_HEADER: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case 0: {
                final int payloadLength = Packets.readInt3(cumulateBuffer);
                updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));

                final int payloadIndex = cumulateBuffer.readerIndex();

                cumulateBuffer.readByte();//1. skip status
                this.statementId = Packets.readInt4(cumulateBuffer);//2. statement_id
                final int numColumns = Packets.readInt2AsInt(cumulateBuffer);//3. num_columns
                final int numParams = Packets.readInt2AsInt(cumulateBuffer);//4. num_params
                cumulateBuffer.readByte(); //5. skip filler
                if (payloadLength > 10) {
                    final int warnings = Packets.readInt2AsInt(cumulateBuffer);//6. warning_count
                    if (warnings > 0) {
                        this.warning = JdbdWarning.create(String.format("produce %s warnings", warnings));
                    }
                    if ((this.negotiatedCapability & Capabilities.CLIENT_OPTIONAL_RESULTSET_METADATA) != 0) {
                        cumulateBuffer.readByte(); //7. skip metadata_follows
                        throw new IllegalStateException("Not support CLIENT_OPTIONAL_RESULTSET_METADATA"); //7. metadata_follows
                    }
                }
                cumulateBuffer.readerIndex(payloadIndex + payloadLength); //avoid tail filler.

                // below read param and column meta data
                this.paramMetas = MySQLColumnMeta.readMetas(cumulateBuffer, numParams, this);
                this.rowMeta = MySQLRowMeta.readForPrepare(cumulateBuffer, numColumns, this);
                taskEnd = handleReadPrepareComplete();
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
     * <p>
     * this method will update {@link #phase}.
     * </p>
     *
     * @return true : task end.
     * @see #readPrepareResponse(ByteBuf)
     */
    private boolean handleReadPrepareComplete() {
        assertPhase(Phase.READ_PREPARE_RESPONSE);
        if (this.paramMetas == null) {
            throw new IllegalStateException("this.paramMetas is null.");
        }
        final ParamSingleStmt stmt = this.stmt;
        final boolean taskEnd;
        if (stmt instanceof MySQLPrepareStmt) {
            final PrepareSink sink = (PrepareSink) this.sink;
            this.phase = Phase.WAIT_FOR_BINDING; // first modify phase to WAIT_FOR_BINDING
            sink.statementSink.success(sink.function.apply(this));
            taskEnd = false;
        } else {
            this.phase = Phase.EXECUTE;
            taskEnd = executeNextGroup();
        }
        return taskEnd;
    }


    /**
     * @see #decode(ByteBuf, Consumer)
     * @see #onError(Throwable)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_close.html">Protocol::COM_STMT_CLOSE</a>
     */
    private ByteBuf createCloseStatementPacket() {
        ByteBuf packet = this.adjutant.allocator().buffer(9);

        Packets.writeInt3(packet, 5);
        packet.writeByte(0);// use 0 sequence_id

        packet.writeByte(Packets.COM_STMT_CLOSE);
        Packets.writeInt4(packet, this.statementId);
        return packet;
    }


    /**
     * <p>
     * modify {@link #phase}
     * </p>
     *
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     * @see #executeNextGroup()
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final boolean traceEnabled = LOG.isTraceEnabled();
        if (traceEnabled) {
            LOG.trace("{} read execute response", this);
        }
        final int header = Packets.getHeaderFlag(cumulateBuffer);
        final boolean taskEnd;
        switch (header) {
            case ErrorPacket.ERROR_HEADER: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                taskEnd = readUpdateResult(cumulateBuffer, serverStatusConsumer);
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
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readFetchResponse(final ByteBuf cumulateBuffer) {
        final int flag = Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() + Packets.HEADER_SIZE);
        final boolean taskEnd;
        if (flag == ErrorPacket.ERROR_HEADER) {
            readErrorPacket(cumulateBuffer);
            taskEnd = true;
        } else {
            taskEnd = false;
        }
        return taskEnd;
    }

    /**
     * @return true : reset occur error,task end.
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html">Protocol::COM_STMT_RESET</a>
     */
    private boolean readResetResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final int flag = Packets.getHeaderFlag(cumulateBuffer);
        final boolean taskEnd;
        switch (flag) {
            case ErrorPacket.ERROR_HEADER: {
                readErrorPacket(cumulateBuffer);
                taskEnd = true;
            }
            break;
            case OkPacket.OK_HEADER: {
                final int payloadLength = Packets.readInt3(cumulateBuffer);
                updateSequenceId(Packets.readInt1AsInt(cumulateBuffer));
                final OkPacket ok;
                ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok);
                this.nextGroupNeedReset = false;
                taskEnd = false;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_STMT_RESET response error,flag[%s].", flag);
        }
        return taskEnd;
    }


    /**
     * @see #readExecuteResponse(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_reset.html">Protocol::COM_STMT_RESET</a>
     */
    private ByteBuf createResetPacket() {
        if (this.paramMetas == null) {
            throw new IllegalStateException("before prepare");
        }
        if (!this.nextGroupNeedReset) {
            throw new IllegalStateException("don't need reset.");
        }
        ByteBuf packet = this.adjutant.allocator().buffer(9);

        Packets.writeInt3(packet, 5);
        packet.writeByte(0);// use 0 sequence id

        packet.writeByte(Packets.COM_STMT_RESET);
        Packets.writeInt4(packet, this.statementId);
        return packet;
    }


    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", expectedPhase));
        }
    }

    /*################################## blow private static method ##################################*/



    /*################################## blow private static inner class ##################################*/

    private static final class MySQLPrepareStmt implements PrepareStmt {

        private final String sql;

        private ParamSingleStmt stmt;

        private MySQLPrepareStmt(String sql) {
            this.sql = sql;
        }

        private void setStmt(ParamSingleStmt stmt) {
            if (this.stmt != null) {
                throw new IllegalStateException("this.stmt is non-null.");
            }
            if (!this.sql.equals(stmt.getSql())) {
                throw new IllegalArgumentException("stmt sql and original sql not match.");
            }
            this.stmt = stmt;
        }

        @Override
        public ParamSingleStmt getStmt() {
            final ParamSingleStmt stmt = this.stmt;
            if (stmt == null) {
                throw new IllegalStateException("this.stmt is null.");
            }
            return stmt;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public int getTimeout() {
            return getStmt().getTimeout();
        }

        @Override
        public int getFetchSize() {
            return getStmt().getFetchSize();
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return getStmt().getImportPublisher();
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return getStmt().getExportSubscriber();
        }

    }

    private static final class PrepareSink implements ResultSink {

        private final MonoSink<PreparedStatement> statementSink;

        private final Function<PrepareTask<MySQLType>, PreparedStatement> function;

        private ResultSink sink;

        private PrepareSink(MonoSink<PreparedStatement> statementSink
                , Function<PrepareTask<MySQLType>, PreparedStatement> function) {
            this.statementSink = statementSink;
            this.function = function;
        }

        private void setSink(ResultSink sink) {
            if (this.sink != null) {
                throw new IllegalStateException("this.sink is non-null.");
            }
            this.sink = sink;
        }

        @Override
        public void error(Throwable e) {
            final ResultSink sink = this.sink;
            if (sink == null) {
                this.statementSink.error(e);
            } else {
                sink.error(e);
            }
        }

        @Override
        public void complete() {
            final ResultSink sink = this.sink;
            if (sink != null) {// if null ,possibly error on  binding
                sink.complete();
            }
        }

        @Override
        public boolean isCancelled() {
            final ResultSink sink = this.sink;
            return sink != null && sink.isCancelled();
        }

        @Override
        public void next(Result result) {
            final ResultSink sink = this.sink;
            if (sink == null) {
                throw new IllegalStateException("this.sink is null");
            }
            sink.next(result);
        }

    }


    enum Phase {
        ERROR_ON_START,
        PREPARED,

        READ_PREPARE_RESPONSE,

        WAIT_FOR_BINDING,

        EXECUTE,
        READ_EXECUTE_RESPONSE,
        READ_RESULT_SET,

        RESET_STMT,
        READ_RESET_RESPONSE,

        FETCH_STMT,
        READ_FETCH_RESPONSE,

        ERROR_ON_BINDING,
        ABANDON_BINDING,

        END
    }


}
