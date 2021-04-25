package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLDatabaseSession;
import io.jdbd.mysql.session.ServerPreparedStatement;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.mysql.stmt.PrepareStmtTask;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.BindableStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.result.*;
import io.jdbd.vendor.stmt.BatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmt;
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
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * <p>  code navigation :
 *     <ol>
 *         <li>decode entrance method : {@link #decode(ByteBuf, Consumer)}</li>
 *         <li>send COM_STMT_PREPARE : {@link #start()} </li>
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
 * @see PrepareLongParameterWriter
 * @see BinaryResultSetReader
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase_ps.html">Prepared Statements</a>
 */
final class ComPreparedTask extends MySQLPrepareCommandTask implements StatementTask, PrepareStmtTask {


    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeUpdate()} method:
     * </p>
     *
     * @see #ComPreparedTask(ParamStmt, MonoSink, MySQLTaskAdjutant)
     * @see ComQueryTask#bindableUpdate(BindableStmt, MySQLTaskAdjutant)
     */
    static Mono<ResultState> update(final ParamStmt stmt, final MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindableStatement#executeQuery()}</li>
     *      <li>{@link BindableStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     *
     * @see #ComPreparedTask(ParamStmt, FluxSink, MySQLTaskAdjutant)
     * @see ComQueryTask#bindableQuery(BindableStmt, MySQLTaskAdjutant)
     */
    static Flux<ResultRow> query(final ParamStmt stmt, final MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeBatch()} method.
     * </p>
     *
     * @see #ComPreparedTask(FluxSink, BatchStmt, MySQLTaskAdjutant)
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    static Flux<ResultState> batchUpdate(final BatchStmt<? extends ParamValue> stmt
            , final MySQLTaskAdjutant adjutant) {
        return Flux.create(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }

        });
    }


    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeAsMulti()} method.
     * </p>
     *
     * @see #ComPreparedTask(BatchStmt, MultiResultSink, MySQLTaskAdjutant)
     * @see ComQueryTask#bindableAsMulti(BatchBindStmt, MySQLTaskAdjutant)
     */
    static ReactorMultiResult asMulti(final BatchStmt<? extends ParamValue> stmt, final MySQLTaskAdjutant adjutant) {
        return JdbdMultiResults.create(adjutant, sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeAsFlux()} method.
     * </p>
     *
     * @see #ComPreparedTask(BatchStmt, MultiResultSink, MySQLTaskAdjutant)
     * @see ComQueryTask#bindableAsFlux(BatchBindStmt, MySQLTaskAdjutant)
     */
    static Flux<SingleResult> asFlux(final BatchStmt<? extends ParamValue> stmt, final MySQLTaskAdjutant adjutant) {
        return JdbdMultiResults.createAsFlux(adjutant, sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }


    /**
     * <p>
     * This method is underlying api of below methods:
     * <ul>
     *     <li>{@link DatabaseSession#prepare(String)}</li>
     *     <li>{@link DatabaseSession#prepare(String, int)}</li>
     * </ul>
     * </p>
     *
     * @see DatabaseSession#prepare(String)
     * @see DatabaseSession#prepare(String, int)
     * @see #ComPreparedTask(MySQLDatabaseSession, MySQLTaskAdjutant, MonoSink, Stmt)
     */
    static Mono<PreparedStatement> prepare(final MySQLDatabaseSession session, final Stmt stmt
            , final MySQLTaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                ComPreparedTask task = new ComPreparedTask(session, adjutant, sink, stmt);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTask.class);

    private final DownstreamSink downstreamSink;

    private int statementId;

    private Phase phase = Phase.PREPARED;

    private MySQLColumnMeta[] parameterMetas = MySQLColumnMeta.EMPTY;

    private int parameterMetaIndex = -1;

    private MySQLColumnMeta[] prepareColumnMetas = MySQLColumnMeta.EMPTY;

    private int columnMetaIndex = -1;

    private List<JdbdException> errorList;


    /**
     * @see #update(ParamStmt, MySQLTaskAdjutant)
     */
    private ComPreparedTask(final ParamStmt stmt, final MonoSink<ResultState> sink, final MySQLTaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);

        this.packetPublisher = createPrepareCommand(stmt);
        this.downstreamSink = new UpdateDownstreamSink(this, stmt, sink);
    }

    /**
     * <p>
     * create a prepare statement task for query.
     * </p>
     *
     * @see #query(ParamStmt, MySQLTaskAdjutant)
     */
    private ComPreparedTask(final ParamStmt wrapper, final FluxSink<ResultRow> sink
            , final MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.packetPublisher = createPrepareCommand(wrapper);
        this.downstreamSink = new QueryDownstreamSink(this, wrapper, sink);
    }

    /**
     * @see #batchUpdate(BatchStmt, MySQLTaskAdjutant)
     */
    private ComPreparedTask(final FluxSink<ResultState> sink, final BatchStmt<? extends ParamValue> wrapper
            , final MySQLTaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.packetPublisher = createPrepareCommand(wrapper);
        this.downstreamSink = new BatchUpdateSink<>(this, wrapper, sink);
    }


    /**
     * @see #asMulti(BatchStmt, MySQLTaskAdjutant)
     * @see #asFlux(BatchStmt, MySQLTaskAdjutant)
     */
    private ComPreparedTask(BatchStmt<? extends ParamValue> stmt, MultiResultSink sink, MySQLTaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.packetPublisher = createPrepareCommand(stmt);
        this.downstreamSink = new BatchMultiResultDownstreamSink<>(this, stmt, sink);
    }

    /**
     * @see #prepare(MySQLDatabaseSession, Stmt, MySQLTaskAdjutant)
     */
    private ComPreparedTask(final MySQLDatabaseSession session, final MySQLTaskAdjutant adjutant
            , MonoSink<PreparedStatement> sink, Stmt stmt)
            throws SQLException {
        super(adjutant);
        this.packetPublisher = createPrepareCommand(stmt);
        this.downstreamSink = new DownstreamAdapter(session, this, sink);
    }


    @Override
    public final int obtainStatementId() {
        return this.statementId;
    }

    @Override
    public final MySQLColumnMeta[] obtainParameterMetas() {
        return Objects.requireNonNull(this.parameterMetas, "this.parameterMetas");
    }

    @Override
    public final ClientProtocolAdjutant obtainAdjutant() {
        return this.adjutant;
    }


    @Override
    public final boolean supportFetch() {
        final DownstreamSink downstreamSink = this.downstreamSink;
        return downstreamSink instanceof FetchAbleDownstreamSink
                && ((FetchAbleDownstreamSink) downstreamSink).getFetchSize() > 0;
    }

    @Override
    public final String toString() {
        return this.getClass().getSimpleName();
    }

    /*################################## blow PrepareStmtTask method ##################################*/

    /**
     * @see PrepareStmtTask#executeUpdate(ParamStmt)
     */
    @Override
    public final Mono<ResultState> executeUpdate(final ParamStmt stmt) {
        return Mono.create(sink -> {
            if (this.adjutant.isActive()) {
                if (this.adjutant.inEventLoop()) {
                    doPreparedUpdate(stmt, sink);
                } else {
                    this.adjutant.execute(() -> doPreparedUpdate(stmt, sink));
                }
            } else {
                sink.error(SessionCloseException.create());
            }
        });
    }

    /**
     * @see PrepareStmtTask#executeQuery(ParamStmt)
     */
    @Override
    public final Flux<ResultRow> executeQuery(ParamStmt stmt) {
        return Flux.create(sink -> {

            if (this.adjutant.isActive()) {
                if (this.adjutant.inEventLoop()) {
                    doPreparedQuery(stmt, sink);
                } else {
                    this.adjutant.execute(() -> doPreparedQuery(stmt, sink));
                }
            } else {
                sink.error(SessionCloseException.create());
            }

        });
    }

    /**
     * @see PrepareStmtTask#executeBatch(BatchStmt)
     */
    @Override
    public final Flux<ResultState> executeBatch(final BatchStmt<? extends ParamValue> stmt) {
        final Flux<ResultState> flux;

        if (stmt.getGroupList().isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = Flux.create(sink -> {

                if (this.adjutant.isActive()) {
                    if (this.adjutant.inEventLoop()) {
                        doPreparedBatchUpdate(stmt, sink);
                    } else {
                        this.adjutant.execute(() -> doPreparedBatchUpdate(stmt, sink));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }

            });
        }
        return flux;
    }

    /**
     * @see PrepareStmtTask#executeAsMulti(BatchStmt)
     */
    @Override
    public final ReactorMultiResult executeAsMulti(BatchStmt<? extends ParamValue> stmt) {
        final ReactorMultiResult result;
        if (stmt.getGroupList().isEmpty()) {
            result = JdbdMultiResults.error(MySQLExceptions.createEmptySqlException());
        } else {
            result = JdbdMultiResults.create(this.adjutant, sink -> {
                if (this.adjutant.isActive()) {
                    if (this.adjutant.inEventLoop()) {
                        doPrepareAsMultiOrFlux(stmt, sink);
                    } else {
                        this.adjutant.execute(() -> doPrepareAsMultiOrFlux(stmt, sink));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }
        return result;
    }

    /**
     * @see PrepareStmtTask#executeAsFlux(BatchStmt)
     */
    @Override
    public final Flux<SingleResult> executeAsFlux(BatchStmt<? extends ParamValue> stmt) {
        final Flux<SingleResult> flux;
        if (stmt.getGroupList().isEmpty()) {
            flux = Flux.error(MySQLExceptions.createEmptySqlException());
        } else {
            flux = JdbdMultiResults.createAsFlux(this.adjutant, sink -> {
                if (this.adjutant.isActive()) {
                    if (this.adjutant.inEventLoop()) {
                        doPrepareAsMultiOrFlux(stmt, sink);
                    } else {
                        this.adjutant.execute(() -> doPrepareAsMultiOrFlux(stmt, sink));
                    }
                } else {
                    sink.error(SessionCloseException.create());
                }
            });
        }
        return flux;
    }

    @Override
    public int getWarnings() {
        final DownstreamSink downstreamSink = this.downstreamSink;
        if (!(downstreamSink instanceof DownstreamAdapter)) {
            throw new IllegalStateException(
                    String.format("%s isn't %s", downstreamSink, DownstreamAdapter.class.getSimpleName()));
        }
        return ((DownstreamAdapter) downstreamSink).warnings;
    }

    /*################################## blow CommunicationTask protected  method ##################################*/

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html">Protocol::COM_STMT_PREPARE</a>
     */
    @Override
    protected Publisher<ByteBuf> start() {

        final Publisher<ByteBuf> publisher;
        publisher = Objects.requireNonNull(this.packetPublisher, "this.packetPublisher");
        this.packetPublisher = null;
        this.phase = Phase.READ_PREPARE_RESPONSE;

        if (LOG.isTraceEnabled()) {
            LOG.trace("start {} with downstream[{}]", this, this.downstreamSink);
        }
        return publisher;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
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
                        if (hasReturnColumns()) {
                            this.phase = Phase.READ_PREPARE_COLUMN_META;
                            continueDecode = PacketUtils.hasOnePacket(cumulateBuffer);
                        } else {
                            taskEnd = handleReadPrepareComplete();
                            continueDecode = false;
                        }
                    } else {
                        continueDecode = false;
                    }
                }
                break;
                case READ_PREPARE_COLUMN_META: {
                    if (readPrepareColumnMeta(cumulateBuffer, serverStatusConsumer)) {
                        taskEnd = handleReadPrepareComplete();
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
                        ((BatchUpdateSink<?>) this.downstreamSink).resetSuccess();
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
            if (this.phase != Phase.CLOSE_STMT) {
                this.phase = Phase.CLOSE_STMT;
                this.packetPublisher = Mono.just(createCloseStatementPacket());
            }
            if (hasError()) {
                this.downstreamSink.error(createException());
            } else {
                this.downstreamSink.complete();
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
            case CLOSE_STMT: {
                throw new IllegalStateException("CLOSE_STMT command send error.", e);
            }
            default: {
                addError(MySQLExceptions.wrap(e));
                this.downstreamSink.error(createException());
                this.packetPublisher = Mono.just(createCloseStatementPacket());
                action = Action.MORE_SEND_AND_END;
            }

        }
        return action;
    }

    @Override
    protected void onChannelClose() {
        if (this.phase != Phase.CLOSE_STMT) {
            this.downstreamSink.error(new SessionCloseException("Database session have closed."));
        }
    }

    /*################################## blow private method ##################################*/

    private Publisher<ByteBuf> createPrepareCommand(Stmt stmt) throws SQLException, JdbdSQLException {
        assertPhase(Phase.PREPARED);
        return Flux.fromIterable(
                PacketUtils.createSimpleCommand(PacketUtils.COM_STMT_PREPARE, stmt
                        , this.adjutant, this::addAndGetSequenceId)
        );
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
     * @see UpdateDownstreamSink#nextUpdate(ResultState)
     */
    private boolean replaceIfNeed(Function<JdbdException, JdbdException> function) {
        final List<JdbdException> errorList = this.errorList;
        boolean success = false;
        if (errorList != null) {
            JdbdException temp;
            final int size = errorList.size();
            for (int i = 0; i < size; i++) {
                temp = function.apply(errorList.get(i));
                if (temp != null) {
                    errorList.set(i, temp);
                    success = true;
                    break;
                }
            }

        }
        return success;
    }

    private boolean containException(Class<? extends JdbdException> errorType) {
        final List<JdbdException> errorList = this.errorList;
        boolean contain = false;
        if (errorList != null) {
            for (JdbdException e : errorList) {
                if (e.getClass() == errorType) {
                    contain = true;
                    break;
                }
            }

        }

        return contain;
    }


    /**
     * @return true: prepare error,task end.
     * @see #decode(ByteBuf, Consumer)
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
                final int warnings = PacketUtils.readInt2AsInt(cumulateBuffer);//6. warning_count
                if (this.downstreamSink instanceof DownstreamAdapter) {
                    ((DownstreamAdapter) this.downstreamSink).warnings = warnings;
                }
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
     * <p>
     * this method will update {@link #phase}.
     * </p>
     *
     * @return true : task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean handleReadPrepareComplete() {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} read prepare complete, downstream[{}]", this, this.downstreamSink);
        }
        final DownstreamSink downstreamSink = this.downstreamSink;
        final boolean taskEnd;
        if (downstreamSink instanceof DownstreamAdapter) {
            taskEnd = false;
            this.phase = Phase.WAIT_PARAMS;
            ((DownstreamAdapter) downstreamSink).emitStatement();
            if (LOG.isTraceEnabled()) {
                LOG.trace("{} wait for parameters, downstream[{}]", this, this.downstreamSink);
            }
        } else {
            this.phase = Phase.EXECUTE;
            if (executeStatement()) {
                taskEnd = true;

                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} create execute command packet failure,task end, downstream[{}]"
                            , this, this.downstreamSink);
                }
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} create execute command packet success, downstream[{}]", this, this.downstreamSink);
                }
                taskEnd = false;
                this.phase = Phase.READ_EXECUTE_RESPONSE;
            }
        }
        return taskEnd;
    }


    /**
     * @return true:read parameter meta end.
     * @see #readPrepareResponse(ByteBuf)
     * @see #decode(ByteBuf, Consumer)
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
     * @see #decode(ByteBuf, Consumer)
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
     * @see #decode(ByteBuf, Consumer)
     * @see #onError(Throwable)
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
     * @see #decode(ByteBuf, Consumer)
     * @see #executeStatement()
     */
    private boolean readExecuteResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_EXECUTE_RESPONSE);

        final boolean traceEnabled = LOG.isTraceEnabled();
        if (traceEnabled) {
            LOG.trace("{} start read execute response, downstream[{}]", this, this.downstreamSink);
        }
        final int header = PacketUtils.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex() + PacketUtils.HEADER_SIZE);
        final boolean taskEnd;
        switch (header) {
            case ErrorPacket.ERROR_HEADER: {
                final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));

                ErrorPacket error = ErrorPacket.readPacket(cumulateBuffer.readSlice(payloadLength)
                        , this.negotiatedCapability, this.adjutant.obtainCharsetError());
                addError(MySQLExceptions.createErrorPacketException(error));
                taskEnd = true;
                if (traceEnabled) {
                    LOG.trace("{} read execute error,{}, downstream[{}]", this, error.getErrorMessage()
                            , this.downstreamSink);
                }
            }
            break;
            case OkPacket.OK_HEADER: {
                final int payloadLength = PacketUtils.readInt3(cumulateBuffer);
                updateSequenceId(PacketUtils.readInt1AsInt(cumulateBuffer));

                OkPacket ok = OkPacket.read(cumulateBuffer.readSlice(payloadLength), this.negotiatedCapability);
                serverStatusConsumer.accept(ok.getStatusFags());
                // emit update result
                taskEnd = this.downstreamSink.nextUpdate(MySQLResultState.from(ok));
                if (traceEnabled) {
                    LOG.trace("{} start read execute update result,haMoreResult[{}], downstream[{}]"
                            , this, this.downstreamSink.hasMoreResult(), this.downstreamSink);
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
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readResultSet(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_RESULT_SET);

        final boolean traceEnabled = LOG.isTraceEnabled();
        if (traceEnabled) {
            LOG.trace("{}  read binary ResultSet, downstream[{}]", this, this.downstreamSink);
        }
        if (!this.downstreamSink.readResultSet(cumulateBuffer, serverStatusConsumer)) {
            return false;
        }
        final boolean taskEnd;
        if (this.downstreamSink.hasMoreResult()) {
            if (traceEnabled) {
                LOG.trace("{}  read ResultSet end,hasMoreResult[{}], downstream[{}]"
                        , this, this.downstreamSink.hasMoreResult(), this.downstreamSink);
            }
            this.phase = Phase.READ_EXECUTE_RESPONSE;
            taskEnd = false;
        } else if (hasError()) {
            taskEnd = true;
        } else if (this.downstreamSink instanceof BatchDownstreamSink) {
            if (this.phase == Phase.READ_RESET_RESPONSE) {
                taskEnd = false;
            } else {
                final BatchDownstreamSink sink = (BatchDownstreamSink) this.downstreamSink;
                taskEnd = !sink.hasMoreGroup() || sink.executeCommand();
            }
        } else {
            // maybe more fetch or reset
            taskEnd = this.phase != Phase.READ_FETCH_RESPONSE;
        }
        return taskEnd;
    }

    /**
     * <p>
     * modify {@link #phase}
     * </p>
     *
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
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
     * @see #decode(ByteBuf, Consumer)
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
                serverStatusConsumer.accept(MySQLResultState.from(ok));
                taskEnd = false;
            }
            break;
            default:
                throw MySQLExceptions.createFatalIoException("COM_STMT_RESET response error,flag[%s].", flag);
        }
        return taskEnd;
    }

    /**
     * @see #decode(ByteBuf, Consumer)
     * @see UpdateDownstreamSink#executeCommand()
     * @see BatchUpdateSink#executeCommand()
     */
    boolean hasReturnColumns() {
        MySQLColumnMeta[] columnMetas = this.prepareColumnMetas;
        return columnMetas != null && columnMetas.length > 0;
    }


    /**
     * @return true : task end:<ul>
     * <li>bind parameter error</li>
     * <li>batch update end</li>
     * </ul>
     * @see #decode(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html">Protocol::COM_STMT_EXECUTE</a>
     */
    private boolean executeStatement() {
        assertPhase(Phase.EXECUTE);
        return this.downstreamSink.executeCommand();
    }

    /**
     * @return true : send occur error,task end.
     * @see DownstreamSink#executeCommand()
     */
    private boolean sendExecuteCommand(ExecuteCommandWriter writer, final int stmtIndex
            , List<? extends ParamValue> parameterGroup) {
        updateSequenceId(-1); // reset sequenceId

        if (LOG.isTraceEnabled()) {
            LOG.trace("{}  send execute command,stmtIndex[{}] downstream[{}]", this, stmtIndex, this.downstreamSink);
        }
        boolean taskEnd = false;
        try {
            this.packetPublisher = writer.writeCommand(stmtIndex, parameterGroup);
            this.phase = Phase.READ_EXECUTE_RESPONSE;
        } catch (Throwable e) {
            addError(MySQLExceptions.wrap(e));
            taskEnd = true;
        }
        return taskEnd;
    }

    /**
     * @see BatchUpdateSink#nextUpdate(ResultState)
     */
    private void sendResetCommand() {
        this.phase = Phase.RESET_STMT;
        this.packetPublisher = Mono.just(createResetPacket());
        this.phase = Phase.READ_RESET_RESPONSE;
    }

    /**
     * @see QueryDownstreamSink#readResultSet(ByteBuf, Consumer)
     */
    private void sendFetchCommand() {
        if (!this.supportFetch()) {
            throw new IllegalStateException(String.format("%s not support fetch command.", this));
        }
        this.phase = Phase.FETCH_STMT;
        this.packetPublisher = Mono.just(createFetchPacket());
        this.phase = Phase.READ_FETCH_RESPONSE;
    }


    /**
     * @see #readResultSet(ByteBuf, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_fetch.html">Protocol::COM_STMT_FETCH</a>
     */
    private ByteBuf createFetchPacket() {
        assertPhase(Phase.FETCH_STMT);
        final DownstreamSink downstreamSink = this.downstreamSink;
        if (!(downstreamSink instanceof FetchAbleDownstreamSink)) {
            throw new IllegalStateException(String.format("%s isn't fetch able downstream.", downstreamSink));
        }

        ByteBuf packet = this.adjutant.allocator().buffer(13);
        PacketUtils.writeInt3(packet, 9);
        packet.writeByte(addAndGetSequenceId());

        packet.writeByte(PacketUtils.COM_STMT_FETCH);
        PacketUtils.writeInt4(packet, this.statementId);
        PacketUtils.writeInt4(packet, ((FetchAbleDownstreamSink) downstreamSink).getFetchSize());
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
        packet.writeByte(0);// use 0 sequence id

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
     * @see #decode(ByteBuf, Consumer)
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


    /**
     * @see #executeUpdate(ParamStmt)
     */
    private void doPreparedUpdate(final ParamStmt stmt, final MonoSink<ResultState> sink) {
        doPreparedExecute(sink::error, () -> new UpdateDownstreamSink(ComPreparedTask.this, stmt, sink));
    }

    /**
     * @see #executeQuery(ParamStmt)
     */
    private void doPreparedQuery(final ParamStmt stmt, final FluxSink<ResultRow> sink) {
        doPreparedExecute(sink::error, () -> new QueryDownstreamSink(ComPreparedTask.this, stmt, sink));
    }

    /**
     * @see #executeBatch(BatchStmt)
     */
    private void doPreparedBatchUpdate(BatchStmt<? extends ParamValue> stmt, FluxSink<ResultState> sink) {
        doPreparedExecute(sink::error, () -> new BatchUpdateSink<>(ComPreparedTask.this, stmt, sink));
    }

    /**
     * @see #executeAsMulti(BatchStmt)
     */
    private void doPrepareAsMultiOrFlux(BatchStmt<? extends ParamValue> stmt, MultiResultSink sink) {
        doPreparedExecute(sink::error, () -> new BatchMultiResultDownstreamSink<>(ComPreparedTask.this, stmt, sink));
    }


    /**
     * @see #doPreparedUpdate(ParamStmt, MonoSink)
     * @see #doPreparedQuery(ParamStmt, FluxSink)
     * @see #doPreparedBatchUpdate(BatchStmt, FluxSink)
     */
    private void doPreparedExecute(final Consumer<Throwable> errorConsumer, final Supplier<DownstreamSink> supplier) {
        final DownstreamSink downstreamSink = this.downstreamSink;

        if (this.phase == Phase.CLOSE_STMT) {
            errorConsumer.accept(new IllegalStateException
                    (String.format("%s closed.", PreparedStatement.class.getSimpleName())));

        } else if (!(downstreamSink instanceof DownstreamAdapter)) {
            errorConsumer.accept(new IllegalStateException(
                    String.format("%s isn't %s.", downstreamSink, DownstreamAdapter.class.getSimpleName())));
        } else {
            final DownstreamAdapter downstreamAdapter = (DownstreamAdapter) downstreamSink;
            try {

                if (downstreamAdapter.downstreamSink != null) {
                    throw new IllegalStateException(String.format("%s downstreamSink duplicate.", this));
                }
                final DownstreamSink actualDownstreamSink = supplier.get();
                if (actualDownstreamSink instanceof DownstreamAdapter) {
                    throw new IllegalArgumentException(String.format("downstreamSink type[%s] error."
                            , downstreamSink.getClass().getSimpleName()));
                }
                if (ComPreparedTask.this.phase != Phase.WAIT_PARAMS) {
                    throw new IllegalStateException(String.format("%s is executing ,reject execute again."
                            , ComPreparedTask.class.getSimpleName()));
                }

                downstreamAdapter.downstreamSink = actualDownstreamSink;
                this.phase = Phase.EXECUTE;
                if (executeStatement()) {
                    this.phase = Phase.CLOSE_STMT;
                    this.packetPublisher = Mono.just(createCloseStatementPacket());
                } else {
                    this.phase = Phase.READ_EXECUTE_RESPONSE;
                }
            } catch (Throwable e) {
                this.phase = Phase.CLOSE_STMT;
                this.packetPublisher = Mono.just(createCloseStatementPacket());
                if (e instanceof IllegalStateException) {
                    errorConsumer.accept(e);
                } else {
                    errorConsumer.accept(MySQLExceptions.wrap(e));
                }
            }

            this.sendPacketSignal(this.phase == Phase.CLOSE_STMT)
                    .doOnError(errorConsumer)
                    .subscribe();

        }


    }


    private void assertPhase(Phase expectedPhase) {
        if (this.phase != expectedPhase) {
            throw new IllegalStateException(String.format("this.phase isn't %s.", expectedPhase));
        }
    }

    /*################################## blow private static method ##################################*/



    /*################################## blow private static inner class ##################################*/


    private interface DownstreamSink {

        /**
         * @return true execute task occur error,task end.
         */
        boolean executeCommand();

        /**
         * @return true : task end.
         */
        boolean nextUpdate(ResultState states);

        /**
         * @return true : ResultSet end
         */
        boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        boolean hasMoreResult();

        void error(JdbdException e);

        void complete();
    }

    private interface BatchDownstreamSink extends DownstreamSink {

        boolean hasMoreGroup();

        void resetSuccess();
    }

    private interface FetchAbleDownstreamSink extends DownstreamSink {

        int getFetchSize();
    }


    private static abstract class AbstractDownstreamSink implements DownstreamSink, ResultRowSink {

        final ComPreparedTask task;

        private ResultSetReader skipResultSetReader;

        private ResultState lastResultState;

        AbstractDownstreamSink(ComPreparedTask task) {
            this.task = task;
        }

        /**
         * @return true : result set end.
         */
        final boolean skipResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            ResultSetReader resultSetReader = this.skipResultSetReader;
            if (resultSetReader == null || resultSetReader.isResettable() != this.isResettable()) {
                resultSetReader = this.createResultSetReader();
                this.skipResultSetReader = resultSetReader;
            }
            return resultSetReader.read(cumulateBuffer, serverStatusConsumer);
        }

        @Override
        public final boolean nextUpdate(ResultState states) {
            this.lastResultState = states;
            return this.internalNextUpdate(states);
        }

        @Override
        public final boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            final boolean resultSetEnd;
            if (this.task.hasError() && this.lastResultSetEnd()) {
                resultSetEnd = this.skipResultSet(cumulateBuffer, serverStatusConsumer);
            } else {
                resultSetEnd = this.internalReadResultSet(cumulateBuffer, serverStatusConsumer);
            }
            return resultSetEnd;
        }

        @Override
        public final void accept(ResultState resultState) {
            this.lastResultState = resultState;
            if (!this.lastResultSetEnd()) {
                this.internalAccept(resultState);
            }
        }

        @Override
        public final boolean hasMoreResult() {
            return Objects.requireNonNull(this.lastResultState, "this.lastResultStatus")
                    .hasMoreResult();
        }

        abstract void internalAccept(ResultState resultState);

        /**
         * @return true: taskEnd.
         */
        abstract boolean internalNextUpdate(ResultState states);

        /**
         * @return true: ResultSet end.
         */
        abstract boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer);

        /**
         * @see #createResultSetReader()
         */
        abstract boolean isResettable();

        @Override
        public final void next(ResultRow resultRow) {
            if (this.skipResultSetReader == null) {
                this.internalNext(resultRow);
            }
        }

        @Override
        public final boolean isCancelled() {
            return this.skipResultSetReader != null
                    || this.task.hasError()
                    || this.internalIsCancelled();
        }

        /**
         * subclass that support query override this method.
         *
         * @see #next(ResultRow)
         */
        void internalNext(ResultRow resultRow) {
            if (this instanceof QueryDownstreamSink
                    || this instanceof BatchMultiResultDownstreamSink) {
                throw new IllegalStateException(String.format("%s not override internalNext method.", this));
            }
        }

        /**
         * subclass that support query override this method.
         *
         * @see #isCancelled()
         */
        boolean internalIsCancelled() {
            if (this instanceof QueryDownstreamSink
                    || this instanceof BatchMultiResultDownstreamSink) {
                throw new IllegalStateException(String.format("%s not override internalIsCancelled method.", this));
            }
            return true;
        }

        /**
         * subclass that support query override this method.
         */
        boolean lastResultSetEnd() {
            if (this instanceof QueryDownstreamSink
                    || this instanceof BatchMultiResultDownstreamSink) {
                throw new IllegalStateException(String.format("%s not override lastResultSetEnd method.", this));
            }
            return true;
        }


        final ResultSetReader createResultSetReader() {
            return ResultSetReaderBuilder
                    .builder()

                    .rowSink(this)
                    .adjutant(task.adjutant)
                    .fetchResult(this.task.supportFetch())
                    .sequenceIdUpdater(this.task::updateSequenceId)

                    .errorConsumer(this.task::addError)
                    .resettable(this.isResettable())
                    .build(BinaryResultSetReader.class);
        }

        final void addCommandSentExceptionIfNeed() {
            final String message = String.format("%s execute command have sent,reject sent command again.", this);
            if (this.task.containException(MySQLJdbdException.class)) {
                if (LOG.isDebugEnabled()) {
                    LOG.error(message);
                }

            } else {
                this.task.addError(new MySQLJdbdException(message, new IllegalStateException(message)));
            }

        }

        @Override
        public final String toString() {
            return this.getClass().getSimpleName();
        }

    }


    /**
     * <p>
     * This class is underlying implementation downstream of below methods:
     *     <ul>
     *         <li>{@link BindableStatement#executeQuery()}</li>
     *         <li>{@link BindableStatement#executeQuery(Consumer)}</li>
     *         <li>{@link PreparedStatement#executeQuery()}</li>
     *         <li>{@link PreparedStatement#executeQuery(Consumer)}</li>
     *     </ul>
     * </p>
     *
     * @see ComPreparedTask#ComPreparedTask(ParamStmt, FluxSink, MySQLTaskAdjutant)
     */
    private static final class QueryDownstreamSink extends AbstractDownstreamSink implements FetchAbleDownstreamSink {

        private final ResultSetReader resultSetReader;

        private final List<? extends ParamValue> parameterGroup;

        private final FluxSink<ResultRow> sink;

        private final Consumer<ResultState> statesConsumer;

        private final int fetchSize;

        /**
         * query result status
         */
        private ResultState queryStatus;

        /**
         * @see ComPreparedTask#ComPreparedTask(ParamStmt, FluxSink, MySQLTaskAdjutant)
         */
        private QueryDownstreamSink(final ComPreparedTask task, ParamStmt wrapper, FluxSink<ResultRow> sink) {
            super(task);
            if (task.properties.getOrDefault(PropertyKey.useCursorFetch, Boolean.class)
                    && Capabilities.supportPsMultiResult(task.negotiatedCapability)) {
                // MySQL only create cursor-backed result sets if
                // a) The query is a SELECT
                // b) The server supports it
                // c) We know it is forward-only (note this doesn't preclude updatable result sets)
                // d) The user has set a fetch size
                this.fetchSize = wrapper.getFetchSize();
            } else {
                this.fetchSize = -1;
            }
            this.resultSetReader = this.createResultSetReader();
            this.parameterGroup = wrapper.getParamGroup();
            this.sink = sink;
            this.statesConsumer = wrapper.getStatusConsumer();
        }

        @Override
        final boolean isResettable() {
            //TODO zoro check fetch response.
            return this.fetchSize > 0;
        }

        @Override
        public final boolean executeCommand() {
            final boolean taskEnd;
            if (this.queryStatus != null) {
                // here bug.
                addCommandSentExceptionIfNeed();
                taskEnd = true;
            } else if (this.task.hasReturnColumns()) {
                PrepareExecuteCommandWriter writer = new PrepareExecuteCommandWriter(this.task);
                taskEnd = this.task.sendExecuteCommand(writer, -1, this.parameterGroup);
            } else {
                if (!this.task.containException(SubscribeException.class)) {
                    this.task.addError(new SubscribeException(ResultType.QUERY, ResultType.UPDATE));
                }
                taskEnd = true;
            }
            return taskEnd;
        }


        @Override
        final boolean internalNextUpdate(final ResultState states) {
            // here ,sql is that call stored procedure,skip rest results.
            if (!this.task.containException(SubscribeException.class)) {
                this.task.addError(TaskUtils.createQueryMultiError());
            }
            return !states.hasMoreResult();
        }


        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            if (!this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                return false;
            }
            final ResultState status = Objects.requireNonNull(this.queryStatus, "this.queryStatus");

            if (this.task.hasError()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} occur error.", this);
                }
                this.queryStatus = null; //clear for next query result.
            } else if (status.hasMoreResult()) {
                if (this.fetchSize > 0) {
                    try {
                        this.statesConsumer.accept(status);
                    } catch (Throwable e) {
                        if (!this.task.containException(ResultStatusConsumerException.class)) {
                            this.task.addError(ResultStatusConsumerException.create(this.statesConsumer, e));
                        }
                    }
                } else if (!this.task.containException(SubscribeException.class)) {
                    // here ,sql is that call stored procedure,skip rest results.
                    this.task.addError(TaskUtils.createQueryMultiError());
                }
                this.queryStatus = null; //clear for next query result.
            } else if (status.hasMoreFetch()) {
                this.task.sendFetchCommand(); // fetch more results.
                this.queryStatus = null; //clear for next query result.
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("{} downstream[{}] read complete.", this.task, this);
                }
            }
            return true;
        }


        @Override
        public final int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            final ResultState resultState = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
            try {
                this.statesConsumer.accept(resultState);
                this.sink.complete();
            } catch (Throwable e) {
                this.sink.error(ResultStatusConsumerException.create(this.statesConsumer, e));
            }
        }


        /**
         * @see ResultRowSink#next(ResultRow)
         */
        @Override
        final void internalNext(ResultRow resultRow) {
            this.sink.next(resultRow);
        }

        /**
         * @see ResultRowSink#isCancelled()
         */
        @Override
        final boolean internalIsCancelled() {
            return this.task.hasError() || this.sink.isCancelled();
        }

        /**
         * @see ResultRowSink#accept(ResultState)
         */
        @Override
        final void internalAccept(final ResultState status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s resultSetStatus non-null.", this));
            }
        }


    } // QueryDownstreamSink

    /**
     * <p>
     * This class is underlying implementation downstream of below methods:
     *     <ul>
     *         <li>{@link BindableStatement#executeUpdate()}</li>
     *         <li>{@link PreparedStatement#executeUpdate()}</li>
     *     </ul>
     * </p>
     *
     * @see #ComPreparedTask(ParamStmt, MonoSink, MySQLTaskAdjutant)
     */
    private final static class UpdateDownstreamSink extends AbstractDownstreamSink {

        private final List<? extends ParamValue> group;

        private final MonoSink<ResultState> sink;

        // update status
        private ResultState updateStatus;

        // query result status
        private ResultState queryStatus;

        /**
         * @see #ComPreparedTask(ParamStmt, MonoSink, MySQLTaskAdjutant)
         */
        private UpdateDownstreamSink(final ComPreparedTask task, ParamStmt wrapper, MonoSink<ResultState> sink) {
            super(task);
            this.group = wrapper.getParamGroup();
            this.sink = sink;
        }

        @Override
        public final boolean executeCommand() {
            final boolean taskEnd;
            if (this.updateStatus != null) {
                // here bug.
                addCommandSentExceptionIfNeed();
                taskEnd = true;
            } else if (this.task.hasReturnColumns()) {
                if (!this.task.containException(SubscribeException.class)) {
                    this.task.addError(new SubscribeException(ResultType.UPDATE, ResultType.QUERY));
                }
                taskEnd = true;
            } else {
                PrepareExecuteCommandWriter writer = new PrepareExecuteCommandWriter(this.task);
                taskEnd = this.task.sendExecuteCommand(writer, -1, this.group);
            }
            return taskEnd;
        }

        @Override
        final boolean internalNextUpdate(final ResultState states) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                taskEnd = !states.hasMoreResult();
            } else if (states.hasMoreResult()) {
                if (!this.task.containException(SubscribeException.class)) {
                    // here sql is call stored procedure
                    this.task.addError(TaskUtils.createUpdateMultiError());
                }
                taskEnd = false;
            } else {
                if (this.updateStatus == null) {
                    this.updateStatus = states;
                } else if (!this.task.containException(SubscribeException.class)) {
                    // here sql is call stored procedure
                    this.task.addError(TaskUtils.createUpdateMultiError());
                }
                taskEnd = true;
            }
            return taskEnd;
        }


        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            // here ,sql is that call stored procedure,skip rest results.
            final boolean resultSetEnd;
            if (this.skipResultSet(cumulateBuffer, serverStatusConsumer)) {
                if (!this.task.containException(SubscribeException.class)) {
                    this.task.addError(TaskUtils.createUpdateMultiError());
                }
                resultSetEnd = true;
            } else {
                resultSetEnd = false;
            }
            return resultSetEnd;
        }

        @Override
        final void internalAccept(final ResultState status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s queryStatus non-null.", this));
            }
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final boolean isResettable() {
            return true;
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.success(Objects.requireNonNull(this.updateStatus, "this.resultStates"));
        }


    } // UpdateDownstreamSink

    /**
     * @see #ComPreparedTask(FluxSink, BatchStmt, MySQLTaskAdjutant)
     */
    private static final class BatchUpdateSink<T extends ParamValue> extends AbstractDownstreamSink
            implements BatchDownstreamSink {

        private final List<List<T>> groupList;

        private final FluxSink<ResultState> sink;

        private final ExecuteCommandWriter commandWriter;

        private int index = 0;

        private boolean lastHasLongData;

        private ResultState queryStatus;

        /**
         * @see #ComPreparedTask(FluxSink, BatchStmt, MySQLTaskAdjutant)
         */
        private BatchUpdateSink(final ComPreparedTask task, BatchStmt<T> wrapper, FluxSink<ResultState> sink) {
            super(task);
            this.groupList = wrapper.getGroupList();
            this.sink = sink;
            this.commandWriter = new PrepareExecuteCommandWriter(task);
        }

        @Override
        public final boolean executeCommand() {
            final boolean taskEnd;
            final int currentIndex = this.index;
            if (currentIndex >= this.groupList.size()) {
                // here bug.
                if (!this.task.containException(MySQLJdbdException.class)) {
                    String message = String.format("%s have ended.", this);
                    this.task.addError(new MySQLJdbdException(message, new IllegalStateException(message)));
                }
                taskEnd = true;
            } else if (currentIndex == 0 && this.task.hasReturnColumns()) {
                this.task.addError(TaskUtils.createBatchUpdateQueryError());
                taskEnd = true;
            } else if (this.lastHasLongData) {
                final String message;
                message = String.format("%s last group has long data and no reset,reject execute command.", this);
                if (this.task.containException(MySQLJdbdException.class)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.error(message);
                    }
                } else {
                    this.task.addError(new MySQLJdbdException(message, new IllegalStateException(message)));
                }
                taskEnd = true;
            } else {
                final int groupIndex = this.index++;
                final List<T> group = this.groupList.get(groupIndex);
                this.lastHasLongData = BindUtils.hasLongData(group);
                taskEnd = this.task.sendExecuteCommand(this.commandWriter, groupIndex, group);
            }
            return taskEnd;
        }

        @Override
        final boolean internalNextUpdate(final ResultState states) {
            final boolean taskEnd;
            if (states.hasMoreResult()) {
                if (!this.task.containException(SubscribeException.class)) {
                    this.task.addError(TaskUtils.createBatchUpdateMultiError());
                }
                taskEnd = false;
            } else if (this.task.hasError()) {
                // if sql is that call stored procedure,skip rest results.
                taskEnd = true;
            } else {
                this.sink.next(states);// drain to downstream

                if (this.hasMoreGroup()) {
                    if (this.lastHasLongData) {
                        taskEnd = false;
                        this.task.sendResetCommand();
                    } else {
                        taskEnd = this.executeCommand();
                    }
                } else {
                    taskEnd = true;
                }
            }
            return taskEnd;
        }

        @Override
        public final void resetSuccess() {
            if (!this.lastHasLongData) {
                throw new IllegalStateException(
                        String.format("%s lastHasLongData is false ,reject update status.", this));
            }
            this.lastHasLongData = false;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            //here, sql is that call stored procedure,skip rest results.
            final boolean resultSetEnd;
            if (this.skipResultSet(cumulateBuffer, serverStatusConsumer)) {
                final ResultState status = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
                if (status.hasMoreResult()) {
                    if (this.task.containException(SubscribeException.class)) {
                        this.task.replaceIfNeed(TaskUtils::replaceAsBatchUpdateMultiError);
                    } else {
                        this.task.addError(TaskUtils.createBatchUpdateMultiError());
                    }
                } else if (!this.task.containException(SubscribeException.class)) {
                    this.task.addError(TaskUtils.createBatchUpdateQueryError());
                }
                resultSetEnd = true;
            } else {
                resultSetEnd = false;
            }
            return resultSetEnd;
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.queryStatus != null;
        }

        @Override
        final boolean isResettable() {
            return true;
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final boolean hasMoreGroup() {
            return this.index < this.groupList.size();
        }

        @Override
        final void internalAccept(final ResultState status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s queryStatus non-null.", this));
            }
        }

        @Override
        public final void complete() {
            if (this.index == this.groupList.size()) {
                this.sink.complete();
            } else {
                String message = String.format(
                        "%s execute error,current index[%s] group size[%s]", this, this.index, this.groupList.size());
                throw new MySQLJdbdException(message, new IllegalStateException(message));
            }
        }

    }// BatchUpdateSink class

    /**
     * <p>
     * This class is underlying api downstream sink of below methods:
     *     <ul>
     *         <li>{@link PreparedStatement#executeAsFlux()}</li>
     *         <li>{@link PreparedStatement#executeAsFlux()}</li>
     *     </ul>
     * </p>
     *
     * @param <T> ParamValue
     */
    private final static class BatchMultiResultDownstreamSink<T extends ParamValue> extends AbstractDownstreamSink
            implements BatchDownstreamSink {

        private final MultiResultSink sink;

        private final List<List<T>> groupList;

        private final ExecuteCommandWriter commandWriter;

        private final ResultSetReader resultSetReader;

        private int index = 0;

        private boolean lastHasLongData;

        private QuerySink querySink;

        /**
         * query result status.
         */
        private ResultState queryStatus;


        /**
         * @see #ComPreparedTask(BatchStmt, MultiResultSink, MySQLTaskAdjutant)
         */
        private BatchMultiResultDownstreamSink(final ComPreparedTask task, BatchStmt<T> wrapper
                , MultiResultSink sink) {
            super(task);
            this.sink = sink;
            this.groupList = wrapper.getGroupList();
            this.commandWriter = new PrepareExecuteCommandWriter(task);
            this.resultSetReader = this.createResultSetReader();
        }

        @Override
        public final boolean executeCommand() {
            final boolean taskEnd;
            final int currentIndex = this.index;
            if (currentIndex >= this.groupList.size()) {
                // here bug.
                if (!this.task.containException(MySQLJdbdException.class)) {
                    String message = String.format("%s have ended,reject execute command again.", this);
                    this.task.addError(new MySQLJdbdException(message, new IllegalStateException(message)));
                }
                taskEnd = true;
            } else if (this.lastHasLongData) {
                final String message;
                message = String.format("%s last group has long data and no reset,reject execute command.", this);
                if (this.task.containException(MySQLJdbdException.class)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.error(message);
                    }
                } else {
                    this.task.addError(new MySQLJdbdException(message, new IllegalStateException(message)));
                }
                taskEnd = true;
            } else {
                final int groupIndex = this.index++;
                final List<T> group = this.groupList.get(groupIndex);
                this.lastHasLongData = BindUtils.hasLongData(group);
                taskEnd = this.task.sendExecuteCommand(this.commandWriter, groupIndex, group);
            }
            return taskEnd;
        }

        @Override
        final boolean internalNextUpdate(final ResultState states) {
            final boolean taskEnd;
            if (this.task.hasError()) {
                // if sql is that call stored procedure,skip rest results.
                taskEnd = !states.hasMoreResult();
            } else {
                // drain to downstream
                this.sink.nextUpdate(states);
                if (states.hasMoreResult()) {
                    taskEnd = false;
                } else if (this.hasMoreGroup()) {
                    if (this.lastHasLongData) {
                        taskEnd = false;
                        this.task.sendResetCommand();
                    } else {
                        taskEnd = this.executeCommand();
                    }
                } else {
                    taskEnd = true;
                }
            }
            return taskEnd;
        }

        @Override
        final boolean internalReadResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {

            QuerySink querySink = this.querySink;
            if (querySink == null) {
                querySink = this.sink.nextQuery();
                this.querySink = querySink;
            }
            if (!this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                return false;
            }
            final ResultState status = Objects.requireNonNull(this.queryStatus, "this.queryStatus");
            this.querySink = null; // clear for next query
            this.queryStatus = null;// clear for next query

            if (!this.task.hasError()) {
                querySink.accept(status); // drain to downstream
                querySink.complete();// clear for next query

                if (!status.hasMoreResult() && this.hasMoreGroup() && !this.sink.isCancelled()) {
                    if (this.lastHasLongData) {
                        this.task.sendResetCommand();
                    } else {
                        this.executeCommand();
                    }
                }
            }
            return true;
        }

        @Override
        final void internalAccept(final ResultState status) {
            if (this.queryStatus == null) {
                this.queryStatus = status;
            } else {
                throw new IllegalStateException(String.format("%s queryStatus non-null.", this));
            }
        }

        @Override
        final void internalNext(ResultRow resultRow) {
            final QuerySink querySink = this.querySink;
            if (querySink == null) {
                throw new NullPointerException(String.format("%s this.querySink", this));
            }
            querySink.next(resultRow);
        }

        @Override
        final boolean internalIsCancelled() {
            final QuerySink querySink = this.querySink;
            if (querySink == null) {
                throw new NullPointerException(String.format("%s this.querySink", this));
            }
            return querySink.isCancelled();
        }

        @Override
        final boolean lastResultSetEnd() {
            return this.querySink == null;
        }

        @Override
        public final void error(JdbdException e) {
            this.sink.error(e);
        }

        @Override
        public final void complete() {
            this.sink.complete();
        }

        @Override
        public final boolean hasMoreGroup() {
            return this.index < this.groupList.size();
        }

        @Override
        public final void resetSuccess() {
            if (!this.lastHasLongData) {
                throw new IllegalStateException(
                        String.format("%s lastHasLongData is false ,reject update status.", this));
            }
            this.lastHasLongData = false;
        }

        @Override
        final boolean isResettable() {
            return true;
        }


    }// BatchMultiResultDownstreamSink


    /**
     * <p>
     * This class is underlying api downstream sink of below methods:
     *     <ul>
     *         <li>{@link DatabaseSession#prepare(String)}</li>
     *         <li>{@link DatabaseSession#prepare(String, int)}</li>
     *     </ul>
     * </p>
     */
    private static final class DownstreamAdapter implements FetchAbleDownstreamSink, BatchDownstreamSink {

        private final MySQLDatabaseSession session;

        private final ComPreparedTask task;

        private final MonoSink<PreparedStatement> sink;

        //non-volatile,because update once and don't update again after emit PreparedStatement.
        private int warnings = -1;

        /**
         * @see #doPreparedExecute(Consumer, Supplier)
         */
        private DownstreamSink downstreamSink;

        /**
         * @see #ComPreparedTask(MySQLDatabaseSession, MySQLTaskAdjutant, MonoSink, Stmt)
         */
        private DownstreamAdapter(MySQLDatabaseSession session, ComPreparedTask task
                , MonoSink<PreparedStatement> sink) {
            this.session = session;
            this.task = task;
            this.sink = sink;
        }

        private void emitStatement() {
            if (this.warnings < 0) {
                throw new IllegalStateException("warnings not set.");
            }
            this.sink.success(ServerPreparedStatement.create(this.session, this.task));
        }


        @Override
        public final boolean executeCommand() {
            return Objects.requireNonNull(this.downstreamSink, "this.downstreamSink")
                    .executeCommand();
        }

        @Override
        public boolean nextUpdate(ResultState states) {
            return Objects.requireNonNull(this.downstreamSink, "this.downstreamSink")
                    .nextUpdate(states);
        }

        @Override
        public boolean readResultSet(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
            return Objects.requireNonNull(this.downstreamSink, "this.downstreamSink")
                    .readResultSet(cumulateBuffer, serverStatusConsumer);
        }

        @Override
        public final void error(JdbdException e) {
            if (this.downstreamSink == null) {
                this.sink.error(e);
            } else {
                this.downstreamSink.error(e);
            }
        }

        @Override
        public final void complete() {
            Objects.requireNonNull(this.downstreamSink, "this.downstreamSink")
                    .complete();
        }

        @Override
        public final boolean hasMoreGroup() {
            final DownstreamSink downstreamSink = Objects.requireNonNull(this.downstreamSink, "this.downstreamSink");
            if (!(downstreamSink instanceof BatchDownstreamSink)) {
                throw new IllegalStateException(
                        String.format("%s isn't %s instance.", this, BatchDownstreamSink.class.getSimpleName()));
            }
            return ((BatchDownstreamSink) downstreamSink).hasMoreGroup();
        }

        @Override
        public final void resetSuccess() {
            final DownstreamSink downstreamSink = Objects.requireNonNull(this.downstreamSink, "this.downstreamSink");
            if (!(downstreamSink instanceof BatchDownstreamSink)) {
                throw new IllegalStateException(
                        String.format("%s isn't %s instance.", this, BatchDownstreamSink.class.getSimpleName()));
            }
            ((BatchDownstreamSink) downstreamSink).resetSuccess();
        }

        @Override
        public final int getFetchSize() {
            final DownstreamSink downstreamSink = Objects.requireNonNull(this.downstreamSink, "this.downstreamSink");
            final int fetchSize;
            if (downstreamSink instanceof FetchAbleDownstreamSink) {
                fetchSize = ((FetchAbleDownstreamSink) downstreamSink).getFetchSize();
            } else {
                fetchSize = 0;
            }
            return fetchSize;
        }

        @Override
        public final boolean hasMoreResult() {
            return Objects.requireNonNull(this.downstreamSink, "this.downstreamSink")
                    .hasMoreResult();
        }

    }// DownstreamAdapter class


    enum Phase {
        PREPARED,
        READ_PREPARE_RESPONSE,
        READ_PREPARE_PARAM_META,
        READ_PREPARE_COLUMN_META,

        WAIT_PARAMS,

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
