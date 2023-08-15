package io.jdbd.postgre.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.SessionCloseException;
import io.jdbd.statement.PreparedStatement;
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

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see SimpleQueryTask
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY"> Extended Query</a>
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html"> Extended Query</a>
 */
final class ExtendedQueryTask extends PgCommandTask implements PrepareTask, ExtendedStmtTask {


    static Mono<ResultStates> update(ParamStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static <R> Flux<R> query(final ParamStmt stmt, Function<CurrentRow, R> func, final TaskAdjutant adjutant) {
        return MultiResults.query(func, stmt.getStatusConsumer(), sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static OrderedFlux executeAsFlux(final ParamStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static Flux<ResultStates> batchUpdate(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static BatchQuery batchQuery(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static MultiResult batchAsMulti(ParamBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static OrderedFlux batchAsFlux(final ParamBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });

    }

    static Mono<PrepareTask> prepare(final String sql, final TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                PgPrepareStmt stmt = new PgPrepareStmt(sql);
                PrepareResultSink resultSink = new PrepareResultSink(sink);
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, resultSink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }

        });
    }




    /*################################## blow Constructor method ##################################*/

    private static final Logger LOG = LoggerFactory.getLogger(ExtendedQueryTask.class);

    private final ExtendedCommandWriter commandWriter;

    final Stmt stmt;

    final ResultSink sink;

    private TaskPhase taskPhase = TaskPhase.NONE;

    private BindPhase bindPhase = BindPhase.NONE;

    private List<DataType> parameterTypeList;

    private ResultRowMeta resultRowMeta;


    private ExtendedQueryTask(Stmt stmt, ResultSink sink, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink);
        this.stmt = stmt;
        this.sink = sink;
        this.commandWriter = PgExtendedCommandWriter.create(this);
    }



    /*################################## blow PrepareStmtTask method ##################################*/

    @Override
    public Mono<ResultStates> executeUpdate(final ParamStmt stmt) {
        return MultiResults.update(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public <R> Flux<R> executeQuery(final ParamStmt stmt, Function<CurrentRow, R> function) {
        return MultiResults.query(function, stmt.getStatusConsumer(), sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public Flux<ResultStates> executeBatchUpdate(final ParamBatchStmt stmt) {
        return MultiResults.batchUpdate(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public BatchQuery executeBatchQuery(final ParamBatchStmt stmt) {
        return MultiResults.batchQuery(this.adjutant, sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public MultiResult executeBatchAsMulti(final ParamBatchStmt stmt) {
        return MultiResults.asMulti(this.adjutant, sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public OrderedFlux executeBatchAsFlux(ParamBatchStmt stmt) {
        return MultiResults.asFlux(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public List<PgType> getParamTypes() {
        return Objects.requireNonNull(this.parameterTypeList, "this.parameterTypeList");
    }

    @Override
    public void suspendTask() {
        if (this.adjutant.inEventLoop()) {
            this.suspendTaskInEventLoop();
        } else {
            this.adjutant.execute(this::suspendTaskInEventLoop);
        }
    }


    @Nullable
    @Override
    public ResultRowMeta getRowMeta() {
        return this.resultRowMeta;
    }

    @Override
    public void closeOnBindError(Throwable error) {
        if (this.adjutant.inEventLoop()) {
            closeOnBindErrorInEventLoop(error);
        } else {
            this.adjutant.execute(() -> closeOnBindErrorInEventLoop(error));
        }
    }


    @Override
    public String getSql() {
        return ((ParamSingleStmt) this.stmt).getSql();
    }

    @Override
    public void next(ResultItem result) {
        this.sink.next(result);
    }

    @Override
    public void abandonBind() {
        if (this.adjutant.inEventLoop()) {
            abandonBindInEventLoop();
        } else {
            this.adjutant.execute(this::abandonBindInEventLoop);
        }
    }

    @Nullable
    @Override
    public Warning getWarning() {
        //TODO Notice
        return null;
    }

    /*################################## blow ExtendedStmtTask method ##################################*/

    @Override
    public ParamSingleStmt getStmt() {
        return (ParamSingleStmt) this.stmt;
    }

    @Override
    public void handleNoExecuteMessage() {
        if (this.taskPhase == TaskPhase.END) {
            return;
        }
        LOG.debug("No execute message sent.end task.");
        this.taskPhase = TaskPhase.BINDING_ERROR;
        this.sendPacketSignal(true) // end task
                .doOnSuccess(inDecodeMethod -> {
                    if (inDecodeMethod) {
                        return;
                    }
                    if (hasError()) {
                        publishError(this.sink::error);
                    } else {
                        this.sink.error(new IllegalStateException("No execute message sent."));
                    }
                })
                .subscribe();// if throw error ,representing task have ended,so ignore error.
    }

    @Nullable
    @Override
    protected Publisher<ByteBuf> start() {

        final Publisher<ByteBuf> publisher;
        switch (this.taskPhase) {// must read this.phase not local variable
            case NONE:
                publisher = this.doStartTask();
                break;
            case SUSPEND: {
                publisher = this.packetPublisher;
                if (publisher == null) {
                    // no bug,never here
                    this.taskPhase = TaskPhase.START_ERROR;
                    addError(new IllegalStateException("command message is null"));
                } else switch (this.bindPhase) {
                    case ABANDON:  // close statement
                    case ERROR_ON_BIND:  // close statement
                    case BIND_END: // execute bind message
                        //no-op
                        break;
                    default:
                        addError(PgExceptions.unexpectedEnum(this.bindPhase));
                }
                this.packetPublisher = null; // must clear
            }
            break;
            default:
                throw PgExceptions.unexpectedEnum(this.taskPhase);
        }
        return publisher;
    }


    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {

        final TaskPhase oldPhase = this.taskPhase;
        boolean taskEnd;
        switch (oldPhase) {
            case START_ERROR:
            case END:
                taskEnd = true;
                break;
            case SUSPEND:
                taskEnd = false;
                break;
            default: {
                taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
                if (!taskEnd && this.taskPhase.isEnd()) {// binding occur or abandon bind
                    taskEnd = true;
                }
            }

        }

        if (taskEnd) {
            switch (this.bindPhase) {
                case ABANDON:
                case ERROR_ON_BIND:
                    break;
                default: {
                    if (oldPhase != TaskPhase.START_ERROR && this.commandWriter.isNeedClose()) {
                        this.packetPublisher = this.commandWriter.closeStatement();
                    }
                }
            }
            this.taskPhase = TaskPhase.END;
            if (hasError()) {
                publishError(this.sink::error);
            } else {
                this.sink.complete();
            }
        }
        return taskEnd;
    }


    @Override
    protected void onChannelClose() {
        if (this.taskPhase.isEnd()) {
            addError(new SessionCloseException("Session unexpected close"));
            publishError(this.sink::error);
        }
    }

    @Override
    protected Action onError(final Throwable e) {

        final Action action;
        if (this.taskPhase.isEnd()) {
            action = Action.TASK_END;
        } else {
            addError(e);
            this.taskPhase = TaskPhase.END;
            if (this.commandWriter.isNeedClose()) {
                closeStatement();
                action = Action.MORE_SEND_AND_END;
            } else {
                action = Action.TASK_END;
            }
            publishError(this.sink::error);
        }
        return action;
    }


    @Override
    Logger getLog() {
        return LOG;
    }

    @Override
    void internalToString(StringBuilder builder) {
        builder.append(",taskPhase:")
                .append(this.taskPhase)
                .append(",bindPhase:")
                .append(this.bindPhase);
    }

    @Override
    boolean isDownstreamCanceled() {
        return this.sink.isCancelled();
    }

    @Override
    boolean handleSelectCommand(final long rowCount) {
        final boolean moreFetch;
        if (this.commandWriter.supportFetch()
                && !this.sink.isCancelled()
                && rowCount >= this.commandWriter.getFetchSize()) {
            moreFetch = true;
            this.packetPublisher = this.commandWriter.fetch();
        } else {
            moreFetch = false;
        }
        return moreFetch;
    }

    @Override
    boolean handlePrepareResponse(List<DataType> paramTypeList, @Nullable ResultRowMeta rowMeta) {
        if (this.taskPhase != TaskPhase.READ_PREPARE_RESPONSE || this.parameterTypeList != null) {
            throw new UnExpectedMessageException("Unexpected ParameterDescription message.");
        }
        this.parameterTypeList = paramTypeList;
        this.resultRowMeta = rowMeta;

        final boolean taskEnd;
        if (this.sink instanceof PrepareResultSink) {

            this.taskPhase = TaskPhase.WAIT_FOR_BIND;
            taskEnd = emitPrepareTask();
        } else {
            this.packetPublisher = this.commandWriter.bindAndExecute();
            this.taskPhase = TaskPhase.READ_EXECUTE_RESPONSE;
            taskEnd = false;
        }
        return taskEnd;
    }

    @Override
    boolean handleClientTimeout() {
        //TODO
        return false;
    }


    /**
     * @see #start()
     */
    @Nullable
    private Publisher<ByteBuf> doStartTask() {
        final ExtendedCommandWriter commandWriter = this.commandWriter;

        Publisher<ByteBuf> publisher;

        try {
            if (commandWriter.isOneRoundTrip()) {
                publisher = commandWriter.executeOneRoundTrip();
                this.taskPhase = TaskPhase.READ_EXECUTE_RESPONSE;
            } else if (commandWriter.isNeedPrepare()) {
                publisher = commandWriter.prepare();
                this.taskPhase = TaskPhase.READ_PREPARE_RESPONSE;
            } else if (emitPrepareTask()) {  // emit PrepareTask wait for binding
                this.taskPhase = TaskPhase.END;
                publisher = null;
            } else switch (this.taskPhase) { // must read this.phase not local variable
                case READ_EXECUTE_RESPONSE: {
                    publisher = this.packetPublisher;
                    assert publisher != null;
                    assert this.bindPhase == BindPhase.BIND_END;
                    this.packetPublisher = null; // must clear
                }
                break;
                case SUSPEND: {
                    assert this.packetPublisher == null;
                    assert this.bindPhase == BindPhase.WAIT_FOR_BIND;
                    publisher = null;
                }
                break;
                default:
                    throw PgExceptions.unexpectedEnum(this.taskPhase);
            }
        } catch (Throwable e) {
            this.taskPhase = TaskPhase.START_ERROR;
            publisher = null;
            addError(e);
        }
        return publisher;
    }

    /**
     * @see #closeOnBindError(Throwable)
     */
    private void closeOnBindErrorInEventLoop(final Throwable error) {
        if (this.taskPhase == TaskPhase.WAIT_FOR_BIND) {
            if (this.commandWriter.isNeedClose()) {
                this.packetPublisher = this.commandWriter.closeStatement();
            }
            this.taskPhase = TaskPhase.BINDING_ERROR;
            this.sendPacketSignal(true)
                    .subscribe();
            this.sink.error(PgExceptions.wrapIfNonJvmFatal(error));

        }
    }


    /**
     * @return true : occur error,can't emit.
     * @see #start()
     */
    private boolean emitPrepareTask() {
        final ResultSink sink = this.sink;
        if (sink instanceof PrepareResultSink) {
            this.bindPhase = BindPhase.WAIT_FOR_BIND;
            ((PrepareResultSink) this.sink).stmtSink.success(this);
        } else {
            String msg = String.format("Unknown %s implementation.", sink.getClass().getName());
            addError(new IllegalArgumentException(msg));
        }
        return hasError();
    }


    /**
     * @see #executeUpdate(ParamStmt)
     * @see #executeQuery(ParamStmt, Function)
     * @see #executeBatchUpdate(ParamBatchStmt)
     * @see #executeBatchAsMulti(ParamBatchStmt)
     * @see #executeBatchAsFlux(ParamBatchStmt)
     */
    private void executeAfterBinding(final ResultSink sink, final ParamSingleStmt stmt) {
        if (this.adjutant.inEventLoop()) {
            executePreparedStmtInEventLoop(sink, stmt);
        } else {
            this.adjutant.execute(() -> executePreparedStmtInEventLoop(sink, stmt));
        }
    }

    /**
     * @see #start()
     * @see #doStartTask()
     * @see #suspendTask()
     */
    private void suspendTaskInEventLoop() {
        if (this.bindPhase != BindPhase.WAIT_FOR_BIND) {
            return;
        }
        switch (this.taskPhase) {
            case NONE: // here , stmt is cached
            case READ_PREPARE_RESPONSE:
                this.taskPhase = TaskPhase.SUSPEND;
                break;
            case SUSPEND:
            default:
                // no-op
        }

    }

    /**
     * @see #abandonBind()
     * @see #start()
     */
    private void abandonBindInEventLoop() {
        if (this.bindPhase != BindPhase.WAIT_FOR_BIND) {
            return;
        }
        this.bindPhase = BindPhase.ABANDON;

        switch (this.taskPhase) {
            case READ_PREPARE_RESPONSE: {
                this.closeStatement();
                this.taskPhase = TaskPhase.END;
            }
            break;
            case SUSPEND: {
                if (this.commandWriter.isNeedClose()) {
                    closeStatement();
                    submit(this::addError); // resume task
                }
            }
            break;
            case NONE: // here , stmt is cached
            case END:
            default:
                // no-op
        }


    }

    private void closeStatement() {
        if (this.commandWriter.isNeedClose()) {
            this.packetPublisher = this.commandWriter.closeStatement();
        }
    }


    /**
     * @see #executeAfterBinding(ResultSink, ParamSingleStmt)
     */
    private void executePreparedStmtInEventLoop(final ResultSink sink, final ParamSingleStmt stmt) {

        if (this.bindPhase != BindPhase.WAIT_FOR_BIND) {
            sink.error(PgExceptions.cannotReuseStatement(PreparedStatement.class));
            return;
        }

        try {
            ((PgPrepareStmt) this.stmt).setActualStmt(stmt);
            ((PrepareResultSink) this.sink).setResultSink(sink);

            this.bindPhase = BindPhase.BIND_END;
            this.packetPublisher = this.commandWriter.bindAndExecute();

            switch (this.taskPhase) {
                case READ_PREPARE_RESPONSE:
                    // no-op
                    break;
                case SUSPEND:
                    this.submit(sink::error);  // resume task
                    break;
                case END:
                case READ_EXECUTE_RESPONSE:
                default:
                    throw PgExceptions.unexpectedEnum(this.taskPhase);
            }
        } catch (Throwable e) {
            this.bindPhase = BindPhase.ERROR_ON_BIND;
            closeOnBindError(PgExceptions.wrap(e));
        }


    }


    private enum BindPhase {

        NONE, // Initial State

        WAIT_FOR_BIND,

        BIND_END,

        ERROR_ON_BIND,

        ABANDON


    }


    private enum TaskPhase {

        NONE, // Initial State

        READ_PREPARE_RESPONSE,
        READ_EXECUTE_RESPONSE,


        START_ERROR,

        SUSPEND,

        END;

        private boolean isEnd() {
            return this == END;
        }
    }


    private static final class PrepareResultSink implements ResultSink {


        private final MonoSink<PrepareTask> stmtSink;

        private ResultSink resultSink;


        private PrepareResultSink(MonoSink<PrepareTask> stmtSink) {
            this.stmtSink = stmtSink;
        }

        private void setResultSink(final ResultSink resultSink) {
            if (this.resultSink != null) {
                throw new IllegalStateException("this.resultSink isn't null");
            }
            this.resultSink = resultSink;
        }


        @Override
        public void error(Throwable e) {
            final ResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                this.stmtSink.error(e);
            } else {
                resultSink.error(e);
            }
        }

        @Override
        public void complete() {
            final ResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                throw createNoFluxResultSinkError();
            }
            resultSink.complete();
        }

        @Override
        public boolean isCancelled() {
            final ResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                throw createNoFluxResultSinkError();
            }
            return resultSink.isCancelled();
        }

        @Override
        public void next(ResultItem result) {
            final ResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                throw createNoFluxResultSinkError();
            }
            resultSink.next(result);
        }

        private static IllegalStateException createNoFluxResultSinkError() {
            return new IllegalStateException("this.resultSink is null");
        }

    }


    private static final class PgPrepareStmt implements PrepareStmt {


        private final String sql;

        private ParamSingleStmt actualTmt;


        private PgPrepareStmt(String sql) {
            this.sql = sql;
        }

        void setActualStmt(final ParamSingleStmt stmt) {
            if (this.actualTmt != null) {
                throw PgExceptions.cannotReuseStatement(PreparedStatement.class);
            }
            if (!this.sql.equals(stmt.getSql())) {
                // no bug ,never here
                throw new IllegalArgumentException("Sql not match,reject update stmt");
            }
            this.actualTmt = stmt;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public ParamSingleStmt getStmt() {
            final ParamSingleStmt stmt = this.actualTmt;
            if (stmt == null) {
                throw new IllegalStateException("this.stmt isn null.");
            }
            return stmt;
        }

        @Override
        public List<NamedValue> getStmtVarList() {
            return this.getStmt().getStmtVarList();
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
        public Function<ChunkOption, Publisher<byte[]>> getImportFunction() {
            return getStmt().getImportFunction();
        }

        @Override
        public Function<ChunkOption, Subscriber<byte[]>> getExportFunction() {
            return getStmt().getExportFunction();
        }

        @Override
        public boolean isSessionCreated() {
            return getStmt().isSessionCreated();
        }

        @Override
        public DatabaseSession databaseSession() {
            return getStmt().databaseSession();
        }

    }//class PgPrepareStmt


}
