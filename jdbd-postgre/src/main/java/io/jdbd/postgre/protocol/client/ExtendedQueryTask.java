package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.SessionCloseException;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
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

    static Flux<ResultRow> query(final ParamStmt stmt, Function<CurrentRow, ResultRow> func, final TaskAdjutant adjutant) {
        return MultiResults.query(func, stmt.getStatusConsumer(), sink -> {
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

    static MultiResult batchAsMulti(BindBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static OrderedFlux batchAsFlux(final BindBatchStmt stmt, final TaskAdjutant adjutant) {
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

    private final ExtendedCommandWriter commandWriter;


    private Phase phase;

    private List<PgType> parameterTypeList;

    private ResultRowMeta resultRowMeta;


    private ExtendedQueryTask(Stmt stmt, ResultSink sink, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink, stmt);
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
        return null;
    }

    /*################################## blow ExtendedStmtTask method ##################################*/

    @Override
    public ParamSingleStmt getStmt() {
        return (ParamSingleStmt) this.stmt;
    }

    @Override
    public void handleNoExecuteMessage() {
        if (this.phase != Phase.END) {
            log.debug("No execute message sent.end task.");
            this.phase = Phase.BINDING_ERROR;
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
    }

    @Nullable
    @Override
    protected Publisher<ByteBuf> start() {
        final ExtendedCommandWriter commandWriter = this.commandWriter;

        Publisher<ByteBuf> publisher;
        try {
            CachePrepare cachePrepare;
            if (commandWriter.isOneShot()) {
                publisher = commandWriter.executeOneShot();
                this.phase = Phase.READ_EXECUTE_RESPONSE;
            } else if ((cachePrepare = commandWriter.getCache()) != null) {
                final ParamSingleStmt stmt = (ParamSingleStmt) this.stmt;
                if (stmt instanceof PrepareStmt) {
                    if (emitPreparedStatement(cachePrepare)) {
                        this.phase = Phase.END;
                    } else {
                        this.phase = Phase.WAIT_FOR_BIND;
                    }
                    publisher = null;
                } else {
                    publisher = commandWriter.bindAndExecute();
                    this.phase = Phase.READ_EXECUTE_RESPONSE;
                }
            } else {
                publisher = commandWriter.prepare();
                this.phase = Phase.READ_PREPARE_RESPONSE;
            }
        } catch (Throwable e) {
            this.phase = Phase.START_ERROR;
            publisher = null;
            addError(e);
        }
        return publisher;
    }

    @Override
    protected void onChannelClose() {
        if (this.phase.isEnd()) {
            addError(new SessionCloseException("Session unexpected close"));
            publishError(this.sink::error);
        }
    }

    @Override
    protected Action onError(Throwable e) {

        final Action action;
        if (this.phase.isEnd()) {
            action = Action.TASK_END;
        } else {
            addError(e);
            if (this.commandWriter.needClose()) {
                this.packetPublisher = this.commandWriter.closeStatement();
                action = Action.MORE_SEND_AND_END;
            } else {
                action = Action.TASK_END;
            }
            publishError(this.sink::error);
        }
        return action;
    }


    @Override
    protected boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {

        final Phase oldPhase = this.phase;
        boolean taskEnd;
        if (oldPhase == Phase.START_ERROR) {
            taskEnd = true;
        } else {
            taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
            if (!taskEnd && this.phase.isEnd()) {// binding occur or abandon bind
                taskEnd = true;
            }
        }
        if (taskEnd) {
            switch (this.phase) {
                case ABANDON_BIND:
                case BINDING_ERROR:
                    break;
                default: {
                    if (oldPhase != Phase.START_ERROR && this.commandWriter.needClose()) {
                        this.packetPublisher = this.commandWriter.closeStatement();
                    }
                }
            }
            this.phase = Phase.END;
            if (hasError()) {
                publishError(this.sink::error);
            } else {
                this.sink.complete();
            }
        }
        return taskEnd;
    }


    @Override
    void internalToString(StringBuilder builder) {
        builder.append(",phase:")
                .append(this.phase);
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
    boolean handlePrepareResponse(List<PgType> paramTypeList, @Nullable ResultRowMeta rowMeta) {
        if (this.phase != Phase.READ_PREPARE_RESPONSE || this.parameterTypeList != null) {
            throw new UnExpectedMessageException("Unexpected ParameterDescription message.");
        }
        this.parameterTypeList = paramTypeList;
        this.resultRowMeta = rowMeta;

        final boolean taskEnd;
        if (this.sink instanceof PrepareResultSink) {
            final long cacheTime = 0;

            final CachePrepareImpl cachePrepare = new CachePrepareImpl(
                    ((ParamSingleStmt) this.stmt).getSql(), this.commandWriter.getReplacedSql()
                    , paramTypeList, this.commandWriter.getStatementName()
                    , rowMeta, cacheTime);

            this.phase = Phase.WAIT_FOR_BIND;
            taskEnd = emitPreparedStatement(cachePrepare);
        } else {
            this.packetPublisher = this.commandWriter.bindAndExecute();
            this.phase = Phase.READ_EXECUTE_RESPONSE;
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
     * @see #closeOnBindError(Throwable)
     */
    private void closeOnBindErrorInEventLoop(final Throwable error) {
        if (this.phase == Phase.WAIT_FOR_BIND) {
            if (this.commandWriter.needClose()) {
                this.packetPublisher = this.commandWriter.closeStatement();
            }
            this.phase = Phase.BINDING_ERROR;
            this.sendPacketSignal(true)
                    .subscribe();
            this.sink.error(PgExceptions.wrapIfNonJvmFatal(error));

        }
    }


    /**
     * @return true : occur error,can't emit.
     * @see #start()
     */
    private boolean emitPreparedStatement(final CachePrepare cache) {
        if (cache instanceof CachePrepareImpl) {
            final ResultSink sink = this.sink;
            if (sink instanceof PrepareResultSink && !hasError()) {
                final PrepareResultSink prepareSink = (PrepareResultSink) this.sink;
                prepareSink.setCachePrepare((CachePrepareImpl) cache);
                prepareSink.stmtSink.success(this);
            } else {
                String msg = String.format("Unknown %s implementation.", sink.getClass().getName());
                addError(new IllegalArgumentException(msg));
            }
        } else {
            String msg = String.format("Unknown %s implementation.", cache.getClass().getName());
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
    private void executeAfterBinding(ResultSink sink, ParamSingleStmt stmt) {
        if (this.adjutant.inEventLoop()) {
            executePreparedStmtInEventLoop(sink, stmt);
        } else {
            this.adjutant.execute(() -> executePreparedStmtInEventLoop(sink, stmt));
        }
    }

    /**
     * @see #abandonBind()
     */
    private void abandonBindInEventLoop(MonoSink<Void> sink) {
        switch (this.phase) {
            case WAIT_FOR_BIND: {
                if (this.commandWriter.needClose()) {
                    this.packetPublisher = this.commandWriter.closeStatement();
                }
                this.phase = Phase.ABANDON_BIND;
                this.sendPacketSignal(true)
                        .subscribe();
                sink.success();
                log.debug("abandonBind success");
            }
            break;
            case END: {
                sink.error(new JdbdException(String.format("%s have ended.", PreparedStatement.class.getName())));
            }
            break;
            default: {
                sink.error(new JdbdException(String.format("%s is executing.", PreparedStatement.class.getName())));
            }

        }


    }


    /**
     * @see #executeAfterBinding(ResultSink, ParamSingleStmt)
     */
    private void executePreparedStmtInEventLoop(ResultSink sink, ParamSingleStmt stmt) {
        if (prepareForPrepareBind(sink, stmt)) {
            // task end
            return;
        }
        this.packetPublisher = this.commandWriter.bindAndExecute();
        this.phase = Phase.READ_EXECUTE_RESPONSE; // modify phase for read response.
        this.sendPacketSignal(false)
                .doOnError(e -> {
                    // signal is rejected for task have ended.
                    addError(e);
                    publishError(this.sink::error);
                })
                .subscribe();

    }


    /**
     * @return true: has error,task end.
     * @see #executePreparedStmtInEventLoop(ResultSink, ParamSingleStmt)
     */
    private boolean prepareForPrepareBind(final ResultSink sink, final ParamSingleStmt stmt) {
        JdbdException error = null;
        switch (this.phase) {
            case WAIT_FOR_BIND: {
                this.phase = Phase.PREPARE_BIND;
                try {
                    final PgPrepareStmt prepareStmt = (PgPrepareStmt) this.stmt;
                    prepareStmt.setActualStmt(stmt);
                    final PrepareResultSink prepareSink = (PrepareResultSink) this.sink;
                    prepareSink.setResultSink(sink);
                } catch (Throwable e) {
                    // here bug
                    error = PgExceptions.wrap(e);
                }
            }
            break;
            case END:
                error = PgExceptions.preparedStatementClosed();
                break;
            default:
                error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        }
        if (error != null) {
            // TODO handle prepare close.
            final JdbdException prepareError = error;
            this.sendPacketSignal(true)
                    .doOnError(e -> {
                        final List<Throwable> errorList = new ArrayList<>(2);
                        errorList.add(prepareError);
                        errorList.add(e);
                        sink.error(PgExceptions.createException(errorList));
                    })
                    .doOnSuccess(v -> sink.error(prepareError))
                    .subscribe();
        }
        return error != null;
    }

    /**
     * @see #getParamTypes()
     * @see #getRowMeta()
     */
    private CachePrepareImpl obtainCachePrepare() {
        final ResultSink sink = this.sink;
        if (!(sink instanceof PrepareResultSink)) {
            throw new IllegalStateException("Not prepare stmt task");
        }
        final PrepareResultSink resultSink = (PrepareResultSink) sink;
        final CachePrepareImpl cachePrepare = resultSink.cachePrepare;
        if (cachePrepare == null) {
            throw new IllegalStateException("Not prepared state.");
        }
        return cachePrepare;
    }


    enum Phase {
        READ_PREPARE_RESPONSE,
        READ_EXECUTE_RESPONSE,
        WAIT_FOR_BIND,
        ABANDON_BIND,
        BINDING_ERROR,
        PREPARE_BIND,
        START_ERROR,
        END;

        private boolean isEnd() {
            return this == END || this == BINDING_ERROR || this == ABANDON_BIND;
        }
    }


    private static final class PrepareResultSink implements ResultSink {


        private final MonoSink<PrepareTask> stmtSink;

        private CachePrepareImpl cachePrepare;

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

        public void setCachePrepare(CachePrepareImpl cachePrepare) {
            if (this.cachePrepare != null) {
                throw new IllegalStateException("this.cachePrepare isn't null");
            }
            this.cachePrepare = cachePrepare;
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

        static PgPrepareStmt prepare(String sql) {
            return new PgPrepareStmt(sql);
        }

        private final String sql;

        private ParamSingleStmt actualTmt;

        private PgPrepareStmt(String sql) {
            this.sql = sql;
        }

        void setActualStmt(final ParamSingleStmt stmt) {
            if (this.actualTmt != null) {
                throw new IllegalStateException("this.stmt isn't null.");
            }
            if (!this.sql.equals(stmt.getSql())) {
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


    }//class PgPrepareStmt


    private static final class CachePrepareImpl implements CachePrepare {

        private final String sql;

        private final String replacedSql;

        private final String prepareName;

        private final List<PgType> paramTypeList;

        private final ResultRowMeta rowMeta;

        private final long cacheTime;


        private CachePrepareImpl(String sql, String replacedSql, List<PgType> paramTypeList
                , String prepareName, @Nullable ResultRowMeta rowMeta, long cacheTime) {
            this.sql = sql;
            this.replacedSql = replacedSql;
            this.paramTypeList = PgCollections.unmodifiableList(paramTypeList);
            this.prepareName = prepareName;
            this.rowMeta = rowMeta;
            this.cacheTime = cacheTime;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public String getReplacedSql() {
            return this.replacedSql;
        }

        @Override
        public List<PgType> getParamOidList() {
            return this.paramTypeList;
        }

        @Override
        public ResultRowMeta getRowMeta() {
            return this.rowMeta;
        }

        @Override
        public long getCacheTime() {
            return this.cacheTime;
        }


    }


}
