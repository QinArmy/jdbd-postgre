package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.SessionCloseException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindBatchStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.PrepareStmtTask;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.FluxResultSink;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.*;
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
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY"> Extended Query</a>
 */
final class ExtendedQueryTask extends AbstractStmtTask implements PrepareStmtTask, ExtendedStmtTask {


    static Mono<ResultStates> update(BindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static Flux<ResultRow> query(BindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static Flux<ResultStates> batchUpdate(BindBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static MultiResult batchAsMulti(BindBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static OrderedFlux batchAsFlux(BindBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });

    }

    static Mono<PreparedStatement> prepare(final String sql, final Function<PrepareStmtTask, PreparedStatement> function
            , final TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                PrepareFluxResultSink resultSink = new PrepareFluxResultSink(function, sink);
                ExtendedQueryTask task = new ExtendedQueryTask(adjutant, PgPrepareStmt.prepare(sql), resultSink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }

        });
    }




    /*################################## blow Constructor method ##################################*/

    private final ExtendedCommandWriter commandWriter;


    private Phase phase;

    /**
     * @see #update(BindStmt, TaskAdjutant)
     * @see #query(BindStmt, TaskAdjutant)
     */
    private ExtendedQueryTask(BindStmt stmt, FluxResultSink sink, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink, stmt);
        this.commandWriter = DefaultExtendedCommandWriter.create(this);
    }

    /**
     * @see #batchUpdate(BindBatchStmt, TaskAdjutant)
     * @see #batchAsMulti(BindBatchStmt, TaskAdjutant)
     * @see #batchAsFlux(BindBatchStmt, TaskAdjutant)
     */
    private ExtendedQueryTask(FluxResultSink sink, BindBatchStmt stmt, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink, stmt);
        this.commandWriter = DefaultExtendedCommandWriter.create(this);
    }

    /**
     * @see #prepare(String, Function, TaskAdjutant)
     */
    private ExtendedQueryTask(TaskAdjutant adjutant, PrepareStmt stmt, PrepareFluxResultSink sink) throws SQLException {
        super(adjutant, sink, stmt);
        this.commandWriter = DefaultExtendedCommandWriter.create(this);
    }


    @Override
    public final Mono<ResultStates> executeUpdate(final ParamStmt stmt) {
        return MultiResults.update(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public final Flux<ResultRow> executeQuery(final ParamStmt stmt) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public final Flux<ResultStates> executeBatch(final ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.batchUpdate(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public final MultiResult executeBatchAsMulti(final ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.asMulti(this.adjutant, sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public final OrderedFlux executeBatchAsFlux(ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.asFlux(sink -> executeAfterBinding(sink, stmt));
    }

    @Override
    public final List<PgType> getParamTypeList() {
        return obtainCachePrepare().paramTypeList;
    }


    @Nullable
    @Override
    public final ResultRowMeta getRowMeta() {
        return obtainCachePrepare().rowMeta;
    }

    @Override
    public final void closeOnBindError(Throwable error) {
        if (this.adjutant.inEventLoop()) {
            closeOnBindErrorInEventLoop(error);
        } else {
            this.adjutant.execute(() -> closeOnBindErrorInEventLoop(error));
        }
    }


    @Override
    public final String getSql() {
        return ((ParamSingleStmt) this.stmt).getSql();
    }

    /*################################## blow ExtendedStmtTask method ##################################*/

    @Override
    public final ParamSingleStmt getStmt() {
        return (ParamSingleStmt) this.stmt;
    }

    @Override
    public final String getNewPortalName() {
        return null;
    }

    @Override
    public final String getStatementName() {
        return null;
    }

    @Override
    public final int getFetchSize() {
        return 0;
    }

    @Override
    public final void appendDescribePortalMessage(List<ByteBuf> messageList) {
    }

    @Override
    public final void handleNoExecuteMessage() {
        if (this.phase != Phase.END) {
            log.debug("No execute message sent.end task.");
            this.sendPacketSignal(true)
                    .subscribe();// if throw error ,representing task have ended,so ignore error.

            if (hasError()) {
                publishError(this.sink::error);
            } else {
                this.sink.error(new IllegalStateException("No execute message sent."));
            }

        }
    }

    @Nullable
    @Override
    protected final Publisher<ByteBuf> start() {
        final ExtendedCommandWriter commandWriter = this.commandWriter;

        Publisher<ByteBuf> publisher;
        try {
            final CachePrepare cachePrepare = commandWriter.getCache();
            if (commandWriter.isOneShot()) {
                publisher = commandWriter.executeOneShot();
                this.phase = Phase.READ_EXECUTE_RESPONSE;
            } else if (cachePrepare != null) {
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
    protected final void onChannelClose() {
        if (this.phase != Phase.END) {
            addError(new SessionCloseException("Session unexpected close"));
            publishError(this.sink::error);
        }
    }

    @Override
    protected final Action onError(Throwable e) {

        final Action action;
        if (this.phase == Phase.END) {
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
    protected final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        final boolean taskEnd;
        if (this.phase == Phase.START_ERROR) {
            taskEnd = true;
        } else {
            taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
        }
        if (taskEnd) {
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
    final void internalToString(StringBuilder builder) {
        builder.append(",phase:")
                .append(this.phase);
    }


    @Override
    final boolean isEndAtReadyForQuery(TxStatus status) {
        final boolean taskEnd;
        switch (this.phase) {
            case READ_PREPARE_RESPONSE: {
                taskEnd = hasError();
            }
            break;
            case READ_EXECUTE_RESPONSE: {
                taskEnd = true;
            }
            break;
            default:
                taskEnd = false;
        }
        return taskEnd;
    }

    @Override
    final void handleSelectCommand(final long rowCount) {
        if (this.commandWriter.supportFetch()
                && !this.sink.isCancelled()
                && rowCount >= this.commandWriter.getFetchSize()) {
            this.packetPublisher = this.commandWriter.fetch();
        }
    }

    @Override
    final void handlePrepareResponse(List<PgType> paramTypeList, @Nullable ResultRowMeta rowMeta) {
        final long cacheTime = 0;

        final CachePrepareImpl cachePrepare = new CachePrepareImpl(
                ((ParamSingleStmt) this.stmt).getSql(), this.commandWriter.getReplacedSql()
                , paramTypeList, this.commandWriter.getStatementName()
                , rowMeta, cacheTime);

        this.phase = Phase.WAIT_FOR_BIND;
        emitPreparedStatement(cachePrepare);
    }

    /**
     * @see #closeOnBindError(Throwable)
     */
    private void closeOnBindErrorInEventLoop(final Throwable error) {
        if (this.phase == Phase.WAIT_FOR_BIND) {
            if (this.commandWriter.needClose()) {
                this.packetPublisher = this.commandWriter.closeStatement();
            }
            this.phase = Phase.END;
            this.sendPacketSignal(true)
                    .subscribe();
            this.sink.error(PgExceptions.wrapIfNonJvmFatal(error));

        }
    }


    /**
     * @return true : task
     * @see #start()
     */
    private boolean emitPreparedStatement(final CachePrepare cache) {
        if (cache instanceof CachePrepareImpl) {
            final FluxResultSink sink = this.sink;
            if (sink instanceof PrepareFluxResultSink && !hasError()) {
                final PrepareFluxResultSink prepareSink = (PrepareFluxResultSink) this.sink;
                prepareSink.setCachePrepare((CachePrepareImpl) cache);
                prepareSink.stmtSink.success(prepareSink.function.apply(this));
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
     * @see #executeQuery(ParamStmt)
     * @see #executeBatch(ParamBatchStmt)
     * @see #executeBatchAsMulti(ParamBatchStmt)
     * @see #executeBatchAsFlux(ParamBatchStmt)
     */
    private void executeAfterBinding(FluxResultSink sink, ParamSingleStmt stmt) {
        if (this.adjutant.inEventLoop()) {
            executePreparedStmtInEventLoop(sink, stmt);
        } else {
            this.adjutant.execute(() -> executePreparedStmtInEventLoop(sink, stmt));
        }
    }


    /**
     * @see #executeAfterBinding(FluxResultSink, ParamSingleStmt)
     */
    private void executePreparedStmtInEventLoop(FluxResultSink sink, ParamSingleStmt stmt) {
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
     * @see #executePreparedStmtInEventLoop(FluxResultSink, ParamSingleStmt)
     */
    private boolean prepareForPrepareBind(final FluxResultSink sink, final ParamSingleStmt stmt) {
        JdbdException error = null;
        switch (this.phase) {
            case WAIT_FOR_BIND: {
                this.phase = Phase.PREPARE_BIND;
                try {
                    final PgPrepareStmt prepareStmt = (PgPrepareStmt) this.stmt;
                    prepareStmt.setActualStmt(stmt);
                    final PrepareFluxResultSink prepareSink = (PrepareFluxResultSink) this.sink;
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

    private void assertPhase(Phase expected) {
        if (this.phase != expected) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't expected[%s]", this.phase, expected));
        }
    }

    /**
     * @see #getParamTypeList()
     * @see #getRowMeta()
     */
    private CachePrepareImpl obtainCachePrepare() {
        final FluxResultSink sink = this.sink;
        if (!(sink instanceof PrepareFluxResultSink)) {
            throw new IllegalStateException("Not prepare stmt task");
        }
        final PrepareFluxResultSink resultSink = (PrepareFluxResultSink) sink;
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
        PREPARE_BIND,
        START_ERROR,
        END
    }


    private static final class PrepareFluxResultSink implements FluxResultSink {

        private final Function<PrepareStmtTask, PreparedStatement> function;

        private final MonoSink<PreparedStatement> stmtSink;

        private CachePrepareImpl cachePrepare;

        private FluxResultSink resultSink;


        private PrepareFluxResultSink(Function<PrepareStmtTask, PreparedStatement> function
                , MonoSink<PreparedStatement> stmtSink) {
            this.function = function;
            this.stmtSink = stmtSink;
        }

        private void setResultSink(final FluxResultSink resultSink) {
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
        public final void error(Throwable e) {
            final FluxResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                this.stmtSink.error(e);
            } else {
                resultSink.error(e);
            }
        }

        @Override
        public final void complete() {
            final FluxResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                throw createNoFluxResultSinkError();
            }
            resultSink.complete();
        }

        @Override
        public final ResultSink froResultSet() {
            return new ResultSink() {
                @Override
                public final boolean isCancelled() {
                    return PrepareFluxResultSink.this.isCancelled();
                }

                @Override
                public final void next(Result result) {
                    PrepareFluxResultSink.this.next(result);
                }
            };
        }

        @Override
        public final boolean isCancelled() {
            final FluxResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                throw createNoFluxResultSinkError();
            }
            return resultSink.isCancelled();
        }

        @Override
        public final void next(Result result) {
            final FluxResultSink resultSink = this.resultSink;
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

        final void setActualStmt(final ParamSingleStmt stmt) {
            if (this.actualTmt != null) {
                throw new IllegalStateException("this.stmt isn't null.");
            }
            if (!this.sql.equals(stmt.getSql())) {
                throw new IllegalArgumentException("Sql not match,reject update stmt");
            }
            this.actualTmt = stmt;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final ParamSingleStmt getStmt() {
            final ParamSingleStmt stmt = this.actualTmt;
            if (stmt == null) {
                throw new IllegalStateException("this.stmt isn null.");
            }
            return stmt;
        }

        @Override
        public final int getTimeout() {
            throw new UnsupportedOperationException();
        }

        @Override
        public final Function<Object, Publisher<byte[]>> getImportPublisher() {
            throw new UnsupportedOperationException();
        }

        @Override
        public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            throw new UnsupportedOperationException();
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
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final String getReplacedSql() {
            return this.replacedSql;
        }

        @Override
        public final List<PgType> getParamOidList() {
            return this.paramTypeList;
        }

        @Override
        public final ResultRowMeta getRowMeta() {
            return this.rowMeta;
        }

        @Override
        public final long getCacheTime() {
            return this.cacheTime;
        }


    }


}
