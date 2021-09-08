package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
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

    static Flux<Result> batchAsFlux(BindBatchStmt stmt, TaskAdjutant adjutant) {
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
                PgPrepareStmt stmt = PgPrepareStmt.prepare(sql);
                PrepareFluxResultSink resultSink = new PrepareFluxResultSink(stmt, function, sink);
                ExtendedQueryTask task = new ExtendedQueryTask(adjutant, resultSink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }

        });
    }




    /*################################## blow Constructor method ##################################*/

    private final ExtendedCommandWriter commandWriter;

    private final String replacedSql;

    private final String prepareName;


    private Phase phase;

    /**
     * @see #update(BindStmt, TaskAdjutant)
     * @see #query(BindStmt, TaskAdjutant)
     */
    private ExtendedQueryTask(BindStmt stmt, FluxResultSink sink, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink, stmt);
        this.prepareName = "";
        this.replacedSql = replacePlaceholder(stmt.getSql());
        this.commandWriter = DefaultExtendedCommandWriter.create(this);
    }

    /**
     * @see #batchUpdate(BindBatchStmt, TaskAdjutant)
     * @see #batchAsMulti(BindBatchStmt, TaskAdjutant)
     * @see #batchAsFlux(BindBatchStmt, TaskAdjutant)
     */
    private ExtendedQueryTask(FluxResultSink sink, BindBatchStmt stmt, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink, stmt);
        this.prepareName = "";
        this.replacedSql = replacePlaceholder(stmt.getSql());
        this.commandWriter = DefaultExtendedCommandWriter.create(this);
    }

    /**
     * @see #prepare(String, Function, TaskAdjutant)
     */
    private ExtendedQueryTask(TaskAdjutant adjutant, PrepareFluxResultSink sink) throws SQLException {
        super(adjutant, sink, sink.prepareStmt);
        this.prepareName = adjutant.createPrepareName();
        this.replacedSql = replacePlaceholder(sink.prepareStmt.sql);
        this.commandWriter = DefaultExtendedCommandWriter.create(this);
    }


    @Override
    public final Mono<ResultStates> executeUpdate(final ParamStmt stmt) {
        return MultiResults.update(sink -> {
            if (this.adjutant.inEventLoop()) {
                executeUpdateInEventLoop(sink, stmt);
            } else {
                this.adjutant.execute(() -> executeUpdateInEventLoop(sink, stmt));
            }
        });
    }

    @Override
    public final Flux<ResultRow> executeQuery(final ParamStmt stmt) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            if (this.adjutant.inEventLoop()) {
                executeQueryInEventLoop(sink, stmt);
            } else {
                this.adjutant.execute(() -> executeQueryInEventLoop(sink, stmt));
            }
        });
    }

    @Override
    public final Flux<ResultStates> executeBatch(final ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.batchUpdate(sink -> {
            if (this.adjutant.inEventLoop()) {
                executeBatchInEventLoop(sink, stmt);
            } else {
                this.adjutant.execute(() -> executeBatchInEventLoop(sink, stmt));
            }
        });
    }

    @Override
    public final MultiResult executeBatchAsMulti(final ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.asMulti(this.adjutant, sink -> {
            if (this.adjutant.inEventLoop()) {
                executeBatchAsMultiInEventLoop(sink, stmt);
            } else {
                this.adjutant.execute(() -> executeBatchAsMultiInEventLoop(sink, stmt));
            }
        });
    }

    @Override
    public final Flux<Result> executeBatchAsFlux(ParamBatchStmt<ParamValue> stmt) {
        return MultiResults.asFlux(sink -> {
            if (this.adjutant.inEventLoop()) {
                executeBatchAsFluxInEventLoop(sink, stmt);
            } else {
                this.adjutant.execute(() -> executeBatchAsFluxInEventLoop(sink, stmt));
            }
        });
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

    }

    @Override
    public final String getSql() {
        return obtainCachePrepare().sql;
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
            this.phase = Phase.END;
            publisher = null;
            addError(e);
        }
        return publisher;
    }


    @Override
    protected final Action onError(Throwable e) {
        return null;
    }

    @Override
    protected final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        final boolean taskEnd;
        if (this.phase == Phase.END) {
            taskEnd = true;
        } else {
            taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
        }
        if (taskEnd) {
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
    final void readOtherMessage(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean continueRead = Messages.hasOneMessage(cumulateBuffer);
        while (continueRead) {
            final int msgStartIndex = cumulateBuffer.readerIndex();
            final int msgType = cumulateBuffer.getByte(msgStartIndex);

            switch (msgType) {
                case Messages.CHAR_ONE: {// ParseComplete message
                    Messages.skipOneMessage(cumulateBuffer);
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.t: {// ParameterDescription message
                    if (this.phase != Phase.READ_PREPARE_RESPONSE) {
                        Messages.skipOneMessage(cumulateBuffer);
                        continue;
                    }
                    if (!Messages.canReadDescribeResponse(cumulateBuffer)) {
                        continueRead = false;
                        continue;
                    }
                    // emit PreparedStatement
                    readPrepareResponseAndEmitStatement(cumulateBuffer);
                    this.phase = Phase.WAIT_FOR_BIND;
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                default: {
                    Messages.skipOneMessage(cumulateBuffer);
                    throw new UnExpectedMessageException(String.format("Server response unknown message type[%s]"
                            , (char) msgType));
                }
            }

        }
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
    final boolean isResultSetPhase() {
        return this.phase == Phase.READ_EXECUTE_RESPONSE;
    }

    /**
     * @see #readOtherMessage(ByteBuf, Consumer)
     */
    private void readPrepareResponseAndEmitStatement(final ByteBuf cumulateBuffer) {
        assertPhase(Phase.READ_PREPARE_RESPONSE);

        final FluxResultSink sink = this.sink;
        if (!(sink instanceof PrepareFluxResultSink)) {
            throw new IllegalStateException("Non Prepare stmt task.");
        }
        final List<PgType> paramTypeList;
        paramTypeList = Messages.readParameterDescription(cumulateBuffer);

        final ResultRowMeta rowMeta;
        switch (cumulateBuffer.getByte(cumulateBuffer.readerIndex())) {
            case Messages.T: {// RowDescription message
                rowMeta = PgRowMeta.readForPrepare(cumulateBuffer, this.adjutant);
            }
            break;
            case Messages.n: {// NoData message
                Messages.skipOneMessage(cumulateBuffer);
                rowMeta = null;
            }
            break;
            default: {
                final char msgType = (char) cumulateBuffer.getByte(cumulateBuffer.readerIndex());
                String m = String.format("Unexpected message[%s] for read prepare response.", msgType);
                throw new UnExpectedMessageException(m);
            }

        }
        final long cacheTime = 0;

        final CachePrepareImpl cachePrepare = new CachePrepareImpl(
                ((ParamSingleStmt) this.stmt).getSql(), this.commandWriter.getReplacedSql()
                , paramTypeList, this.prepareName
                , rowMeta, cacheTime);

        emitPreparedStatement(cachePrepare);
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
     */
    private void executeUpdateInEventLoop(final FluxResultSink sink, final ParamStmt stmt) {
        if (prepareForPrepareBind(sink, stmt)) {
            // task end
            return;
        }
    }


    /**
     * @see #executeQuery(ParamStmt)
     */
    private void executeQueryInEventLoop(FluxResultSink sink, ParamStmt stmt) {
        if (prepareForPrepareBind(sink, stmt)) {
            // task end
            return;
        }
    }

    /**
     * @see #executeBatch(ParamBatchStmt)
     */
    private void executeBatchInEventLoop(FluxResultSink sink, ParamBatchStmt<ParamValue> stmt) {
        if (prepareForPrepareBind(sink, stmt)) {
            // task end
            return;
        }
    }

    /**
     * @see #executeBatchAsMulti(ParamBatchStmt)
     */
    private void executeBatchAsMultiInEventLoop(FluxResultSink sink, ParamBatchStmt<ParamValue> stmt) {
        if (prepareForPrepareBind(sink, stmt)) {
            // task end
            return;
        }
    }

    /**
     * @see #executeBatchAsFlux(ParamBatchStmt)
     */
    private void executeBatchAsFluxInEventLoop(FluxResultSink sink, ParamBatchStmt<ParamValue> stmt) {
        if (prepareForPrepareBind(sink, stmt)) {
            // task end
            return;
        }


    }


    /**
     * @return true: has error,task end.
     * @see #executeUpdateInEventLoop(FluxResultSink, ParamStmt)
     * @see #executeQueryInEventLoop(FluxResultSink, ParamStmt)
     * @see #executeBatchInEventLoop(FluxResultSink, ParamBatchStmt)
     * @see #executeBatchAsMultiInEventLoop(FluxResultSink, ParamBatchStmt)
     * @see #executeBatchAsFluxInEventLoop(FluxResultSink, ParamBatchStmt)
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


    private String replacePlaceholder(final String originalSql) throws SQLException {

        final List<String> staticSqlList = this.adjutant.sqlParser().parse(originalSql).getStaticSql();
        String sql;
        if (staticSqlList.size() == 1) {
            sql = originalSql;
        } else {
            final StringBuilder builder = new StringBuilder(originalSql.length() + staticSqlList.size());
            final int size = staticSqlList.size();
            for (int i = 0; i < size; i++) {
                builder.append(staticSqlList.get(i))
                        .append('$')
                        .append(i + 1);
            }
            builder.append(staticSqlList.get(size - 1));
            sql = builder.toString();
        }
        return sql;
    }


    enum Phase {
        READ_PREPARE_RESPONSE,
        READ_EXECUTE_RESPONSE,
        WAIT_FOR_BIND,
        PREPARE_BIND,
        END
    }


    private static final class PrepareFluxResultSink implements FluxResultSink {

        private final PgPrepareStmt prepareStmt;

        private final Function<PrepareStmtTask, PreparedStatement> function;

        private final MonoSink<PreparedStatement> stmtSink;

        private CachePrepareImpl cachePrepare;

        private FluxResultSink resultSink;


        private PrepareFluxResultSink(PgPrepareStmt prepareStmt
                , Function<PrepareStmtTask, PreparedStatement> function
                , MonoSink<PreparedStatement> stmtSink) {
            this.prepareStmt = prepareStmt;
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
            final FluxResultSink resultSink = this.resultSink;
            if (resultSink == null) {
                throw createNoFluxResultSinkError();
            }
            return resultSink.froResultSet();
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
