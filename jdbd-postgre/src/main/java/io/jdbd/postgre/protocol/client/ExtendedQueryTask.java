package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.PrepareStmtTask;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.FluxResultSink;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.PrepareStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY"> Extended Query</a>
 */
final class ExtendedQueryTask extends AbstractStmtTask implements PrepareStmtTask {


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

    static Flux<ResultStates> batchUpdate(BatchBindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static MultiResult batchAsMulti(BatchBindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                ExtendedQueryTask task = new ExtendedQueryTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static Flux<Result> batchAsFlux(BatchBindStmt stmt, TaskAdjutant adjutant) {
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
    }

    /**
     * @see #batchUpdate(BatchBindStmt, TaskAdjutant)
     * @see #batchAsMulti(BatchBindStmt, TaskAdjutant)
     * @see #batchAsFlux(BatchBindStmt, TaskAdjutant)
     */
    private ExtendedQueryTask(FluxResultSink sink, BatchBindStmt stmt, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink, stmt);
        this.prepareName = "";
        this.replacedSql = replacePlaceholder(stmt.getSql());
    }

    /**
     * @see #prepare(String, Function, TaskAdjutant)
     */
    private ExtendedQueryTask(TaskAdjutant adjutant, PrepareFluxResultSink sink) throws SQLException {
        super(adjutant, sink, sink.prepareStmt);
        this.prepareName = adjutant.createPrepareName();
        this.replacedSql = replacePlaceholder(sink.prepareStmt.sql);
    }


    @Override
    public final Mono<ResultStates> executeUpdate(BindStmt stmt) {
        return null;
    }

    @Override
    public final Flux<ResultRow> executeQuery(BindStmt stmt) {
        return null;
    }

    @Override
    public final Flux<ResultStates> executeBatch(BatchBindStmt stmt) {
        return null;
    }

    @Override
    public final MultiResult executeAsMulti(BatchBindStmt stmt) {
        return null;
    }

    @Override
    public final Flux<Result> executeAsFlux(BatchBindStmt stmt) {
        return null;
    }

    @Override
    public final List<Integer> getParamTypeOidList() {
        return obtainCachePrepare().paramOidList;
    }

    @Nullable
    @Override
    public final ResultRowMeta getRowMeta() {
        return obtainCachePrepare().rowMeta;
    }

    @Override
    public final void closeOnBindError() {

    }

    @Override
    public final String getSql() {
        return obtainCachePrepare().sql;
    }

    @Override
    protected final Publisher<ByteBuf> start() {
        Publisher<ByteBuf> publisher;
        try {
            final Stmt stmt = this.stmt;
            if (stmt instanceof PgPrepareStmt) {
                final List<ByteBuf> messageList = new ArrayList<>(2);
                messageList.add(createParseMessage(((PgPrepareStmt) stmt).sql));
                writeDescribeMessage(messageList, true);
                publisher = Flux.fromIterable(messageList);
                this.phase = Phase.READ_PREPARE_RESPONSE;
            } else {
                throw new IllegalStateException("TODO");
            }
        } catch (Throwable e) {
            publisher = null;
            this.phase = Phase.END;
            this.sink.error(PgExceptions.wrapIfNonJvmFatal(e));
        }
        return publisher;
    }


    @Override
    protected final Action onError(Throwable e) {
        return null;
    }

    @Override
    final void internalToString(StringBuilder builder) {
        builder.append(",phase:")
                .append(this.phase);
    }

    @Override
    final boolean readOtherMessage(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        boolean taskEnd = false, continueRead = Messages.hasOneMessage(cumulateBuffer);
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
        return taskEnd;
    }

    @Override
    final boolean isEndAtReadyForQuery(TxStatus status) {
        return false;
    }

    @Override
    final boolean isResultSetPhase() {
        return this.phase == Phase.READ_EXECUTE_RESPONSE;
    }

    /**
     * @see #readOtherMessage(ByteBuf, Consumer)
     */
    private void readPrepareResponseAndEmitStatement(final ByteBuf cumulateBuffer) {
        final FluxResultSink sink = this.sink;
        if (!(sink instanceof PrepareFluxResultSink)) {
            throw new IllegalStateException("Non Prepare stmt task.");
        }
        final List<Integer> paramOidList = Messages.readParameterDescription(cumulateBuffer);

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

        final PrepareFluxResultSink prepareSink = (PrepareFluxResultSink) sink;
        final long cacheTime = 0;

        final CachePrepareImpl cachePrepare = new CachePrepareImpl(
                prepareSink.prepareStmt.sql, this.replacedSql
                , paramOidList, this.prepareName
                , rowMeta, cacheTime);

        prepareSink.setCachePrepare(cachePrepare);
        prepareSink.stmtSink.success(prepareSink.function.apply(this)); // emit PreparedStatement
    }

    private void assertPhase(Phase expected) {
        if (this.phase != expected) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't expected[%s]", this.phase, expected));
        }
    }

    /**
     * @see #getParamTypeOidList()
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


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Sync</a>
     */
    private void writeSyncMessage(final List<ByteBuf> messageList) {

        final int messageSize = messageList.size();
        final ByteBuf message;
        if (messageSize > 0) {
            final ByteBuf lastMessage = messageList.get(messageSize - 1);
            if (lastMessage.isReadOnly() || lastMessage.writableBytes() < 5) {
                message = this.adjutant.allocator().buffer(5);
                messageList.add(message);
            } else {
                message = lastMessage;
            }
        } else {
            message = this.adjutant.allocator().buffer(5);
            messageList.add(message);
        }
        message.writeByte(Messages.S);
        message.writeInt(0);

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Describe</a>
     */
    private void writeDescribeMessage(final List<ByteBuf> messageList, final boolean statement) {
        final byte[] nameBytes;
        if (this.prepareName.equals("")) {
            nameBytes = new byte[0];
        } else {
            nameBytes = this.prepareName.getBytes(this.adjutant.clientCharset());
        }
        final int length = 6 + nameBytes.length, needCapacity = 1 + length;

        final ByteBuf message;
        if (messageList.size() > 0) {
            final ByteBuf lastMessage = messageList.get(messageList.size() - 1);
            if (lastMessage.isReadOnly() || needCapacity > lastMessage.writableBytes()) {
                message = this.adjutant.allocator().buffer(needCapacity);
                messageList.add(message);
            } else {
                message = lastMessage;
            }
        } else {
            message = this.adjutant.allocator().buffer(needCapacity);
            messageList.add(message);
        }

        message.writeByte(Messages.D);
        message.writeInt(length);
        message.writeByte(statement ? 'S' : 'P');
        message.writeBytes(nameBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Parse</a>
     */
    private ByteBuf createParseMessage(final String originalSql) throws SQLException {
        final Charset charset = this.adjutant.clientCharset();

        final byte[] replacedSqlBytes = replacePlaceholder(originalSql).getBytes(charset);
        ByteBuf message = this.adjutant.allocator().buffer(7 + replacedSqlBytes.length);
        //  write Parse message
        message.writeByte(Messages.P);
        message.writeZero(Messages.LENGTH_BYTES); // placeholder of length
        // write name of the destination prepared statement
        if (!this.prepareName.isEmpty()) {
            message.writeBytes(this.prepareName.getBytes(charset));
        }
        message.writeByte(Messages.STRING_TERMINATOR);
        // write The query string to be parsed.
        message.writeBytes(replacedSqlBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return message;
    }


    private byte[] createPrepareName(final String originalSql) {
        // TODO cache ,write name of prepared statement
        return new byte[0];
    }


    enum Phase {
        READ_PREPARE_RESPONSE,
        READ_EXECUTE_RESPONSE,
        WAIT_FOR_BIND,
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

        private Stmt actualTmt;

        private PgPrepareStmt(String sql) {
            this.sql = sql;
        }

        final void setActualStmt(Stmt stmt) {
            if (this.actualTmt != null) {
                throw new IllegalStateException("this.stmt isn't null.");
            }
            final String stmtSql;
            if (stmt instanceof BindStmt) {
                stmtSql = ((BindStmt) stmt).getSql();
            } else if (stmt instanceof BatchBindStmt) {
                stmtSql = ((BatchBindStmt) stmt).getSql();
            } else {
                throw new IllegalArgumentException(String.format("Unsupported stmt type[%s]", stmt.getClass().getName()));
            }
            if (!this.sql.equals(stmtSql)) {
                throw new IllegalArgumentException("Sql not match,reject update stmt");
            }
            this.actualTmt = stmt;
        }

        @Override
        public final Stmt getStmt() {
            final Stmt stmt = this.actualTmt;
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

        private final List<Integer> paramOidList;

        private final ResultRowMeta rowMeta;

        private final long cacheTime;


        private CachePrepareImpl(String sql, String replacedSql, List<Integer> paramOidList
                , String prepareName, @Nullable ResultRowMeta rowMeta, long cacheTime) {
            this.sql = sql;
            this.replacedSql = replacedSql;
            if (paramOidList.size() == 1) {
                this.paramOidList = Collections.singletonList(paramOidList.get(0));
            } else {
                this.paramOidList = Collections.unmodifiableList(paramOidList);
            }
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
        public final List<Integer> getParamOidList() {
            return this.paramOidList;
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
