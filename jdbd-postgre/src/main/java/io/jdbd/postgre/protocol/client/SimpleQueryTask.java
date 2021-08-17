package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.stmt.BindableStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.FluxResultSink;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSetReader;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.function.Consumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class SimpleQueryTask extends PgTask implements StmtTask {

    /*################################## blow for static stmt ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     */
    static Mono<ResultState> update(Stmt stmt, TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeQuery(String)} method.
     * </p>
     */
    static Flux<ResultRow> query(Stmt stmt, TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeBatch(java.util.List)} method.
     * </p>
     */
    static Flux<ResultState> batchUpdate(GroupStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(java.util.List)} method.
     * </p>
     */
    static MultiResult staticAsMulti(GroupStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(java.util.List)} method.
     * </p>
     */
    static Flux<Result> staticAsFlux(GroupStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /*################################## blow for bindable single stmt ##################################*/

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeUpdate()} method.
     * </p>
     */
    static Mono<ResultState> bindableUpdate(BindableStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindableStatement#executeQuery()}</li>
     *     <li>{@link BindableStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     */
    static Flux<ResultRow> bindableQuery(BindableStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(sink, stmt, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of {@link BindableStatement#executeBatch()} method.
     * </p>
     */
    static Flux<ResultState> bindableBatchUpdate(BatchBindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(adjutant, sink, stmt);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /*################################## blow for bindable multi stmt ##################################*/

    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindableStatement#executeAsMulti()}.
     * </p>
     */
    static MultiResult bindableAsMulti(BatchBindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(adjutant, sink, stmt);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is one of underlying api of below methods {@link BindableStatement#executeAsFlux()}.
     * </p>
     */
    static Flux<Result> bindableAsFlux(BatchBindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(adjutant, sink, stmt);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }



    /*################################## blow for multi stmt  ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsMulti()} method.
     * </p>
     */
    static MultiResult multiStmtAsMulti(MultiBindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(adjutant, stmt, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeAsFlux()} method.
     * </p>
     */
    static Flux<Result> multiStmtAsFlux(MultiBindStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(adjutant, stmt, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(SimpleQueryTask.class);


    private final FluxResultSink sink;

    private final ResultSetReader resultSetReader;

    private int readerIndexBeforeError;

    private int resultIndex = 0;

    private boolean downstreamCanceled;

    private Phase phase;

    /**
     * <p>
     * create instance for single static statement.
     * </p>
     *
     * @see #update(Stmt, TaskAdjutant)
     * @see #query(Stmt, TaskAdjutant)
     */
    private SimpleQueryTask(Stmt stmt, FluxResultSink sink, TaskAdjutant adjutant) throws Throwable {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createStaticSingleCommand(stmt, adjutant);
        this.sink = sink;
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /**
     * @see #batchUpdate(GroupStmt, TaskAdjutant)
     * @see #staticAsMulti(GroupStmt, TaskAdjutant)
     * @see #staticAsFlux(GroupStmt, TaskAdjutant)
     */
    private SimpleQueryTask(GroupStmt stmt, FluxResultSink sink, TaskAdjutant adjutant)
            throws Throwable {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createStaticBatchCommand(stmt, adjutant);
        this.sink = sink;
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /*################################## blow for bindable single stmt ##################################*/

    /**
     * @see #bindableUpdate(BindableStmt, TaskAdjutant)
     * @see #bindableQuery(BindableStmt, TaskAdjutant)
     */
    private SimpleQueryTask(FluxResultSink sink, BindableStmt stmt, TaskAdjutant adjutant)
            throws Throwable {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createBindableCommand(stmt, adjutant);
        this.sink = sink;
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /**
     * @see #bindableBatchUpdate(BatchBindStmt, TaskAdjutant)
     * @see #bindableAsMulti(BatchBindStmt, TaskAdjutant)
     * @see #bindableAsFlux(BatchBindStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, FluxResultSink sink, BatchBindStmt stmt)
            throws Throwable {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createBindableBatchCommand(stmt, adjutant);
        this.sink = sink;
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /*################################## blow for bindable multi stmt ##################################*/

    /**
     * @see #multiStmtAsMulti(MultiBindStmt, TaskAdjutant)
     * @see #multiStmtAsFlux(MultiBindStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, MultiBindStmt stmt, FluxResultSink sink)
            throws Throwable {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createMultiStmtCommand(stmt, adjutant);
        this.sink = sink;
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /*################################## blow io.jdbd.postgre.protocol.client.StmtTask method ##################################*/

    @Override
    public final void addResultSetError(JdbdException error) {
        addError(error);
    }

    @Override
    public final TaskAdjutant adjutant() {
        return this.adjutant;
    }

    @Override
    public final int getAndIncrementResultIndex() {
        return this.resultIndex++;
    }


    @Override
    protected final Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher == null) {
            this.phase = Phase.END;
            this.sink.error(new PgJdbdException("No found command message publisher."));
        } else {
            this.phase = Phase.READ_COMMAND_RESPONSE;
            this.packetPublisher = null;
        }
        return publisher;
    }

    @Override
    protected final boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (!Messages.hasOneMessage(cumulateBuffer)) {
            return false;
        }

        this.readerIndexBeforeError = cumulateBuffer.readerIndex();

        boolean taskEnd = false, continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_COMMAND_RESPONSE: {
                    taskEnd = readCommandResponse(cumulateBuffer, serverStatusConsumer);
                    continueRead = !taskEnd && Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case READ_ROW_SET: {
                    if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                        this.phase = Phase.READ_COMMAND_RESPONSE;
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case END:
                    throw new IllegalStateException("Task have ended.");
                default: {
                    throw PgExceptions.createUnknownEnumException(this.phase);
                }
            }
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
    protected final boolean canDecode(ByteBuf cumulateBuffer) {
        return true;
    }

    @Override
    protected final void onChannelClose() {
        super.onChannelClose();
    }

    @Override
    protected final Action onError(Throwable e) {
        return null;
    }


    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readCommandResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_COMMAND_RESPONSE);

        final boolean isCanceled;
        if (this.downstreamCanceled || hasError()) {
            isCanceled = true;
        } else if (this.sink.isCancelled()) {
            LOG.trace("Downstream cancel subscribe.");
            isCanceled = true;
            this.downstreamCanceled = true;
        } else {
            isCanceled = false;
        }

        final Charset clientCharset = this.adjutant.clientCharset();

        boolean taskEnd = false, continueRead = true;
        while (continueRead) {
            final int msgStartIndex = cumulateBuffer.readerIndex();
            final int msgType = cumulateBuffer.getByte(msgStartIndex);
            final int nextMsgIndex = msgStartIndex + 1 + cumulateBuffer.getInt(msgStartIndex + 1);

            if (isCanceled) {
                if (msgType == Messages.Z) {// ReadyForQuery message
                    serverStatusConsumer.accept(TxStatus.read(cumulateBuffer));
                    taskEnd = true;
                    continueRead = false;
                    readNoticeAfterReadyForQuery(cumulateBuffer, serverStatusConsumer);
                } else {
                    cumulateBuffer.readerIndex(nextMsgIndex);
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                continue;
            }
            switch (msgType) {
                case Messages.E: {// ErrorResponse message
                    ErrorMessage error = ErrorMessage.read(cumulateBuffer, clientCharset);
                    addError(PgExceptions.createErrorException(error));
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.Z: {// ReadyForQuery message
                    serverStatusConsumer.accept(TxStatus.read(cumulateBuffer));
                    taskEnd = true;
                    continueRead = false;
                    readNoticeAfterReadyForQuery(cumulateBuffer, serverStatusConsumer);
                }
                break;
                case Messages.I: {// EmptyQueryResponse message
                    final ResultSetStatus status = Messages.getResultSetStatus(cumulateBuffer);
                    if (status == ResultSetStatus.MORE_CUMULATE) {
                        continueRead = false;
                    } else {
                        final PgResultStates states;
                        final boolean moreResult = status == ResultSetStatus.MORE_RESULT;
                        states = PgResultStates.empty(getAndIncrementResultIndex(), moreResult);
                        this.sink.next(states);
                        cumulateBuffer.readerIndex(nextMsgIndex); // avoid tail filler
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    }
                }
                break;
                case Messages.C: {// CommandComplete message
                    continueRead = readCommandComplete(cumulateBuffer) && Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.T: {// RowDescription message
                    this.phase = Phase.READ_ROW_SET;
                    if (this.resultSetReader.read(cumulateBuffer, serverStatusConsumer)) {
                        this.phase = Phase.READ_COMMAND_RESPONSE;
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case Messages.N: {// NoticeResponse message
                    NoticeMessage noticeMessage = NoticeMessage.read(cumulateBuffer, clientCharset);
                    serverStatusConsumer.accept(noticeMessage);
                    //TODO validate here
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                default: {
                    throw new PgJdbdException(String.format("Server response unknown message type[%s]"
                            , (char) msgType));
                }


            } //  switch (msgType)

        }

        return taskEnd;
    }

    /**
     * @return true: read CommandComplete message end , false : more cumulate.
     * @see #readCommandResponse(ByteBuf, Consumer)
     */
    private boolean readCommandComplete(final ByteBuf cumulateBuffer) {
        final ResultSetStatus status = Messages.getResultSetStatus(cumulateBuffer);
        if (status == ResultSetStatus.MORE_CUMULATE) {
            return false;
        }
        final Charset clientCharset = this.adjutant.clientCharset();
        final int msgStartIndex = cumulateBuffer.readerIndex();
        if (cumulateBuffer.readByte() != Messages.C) {
            cumulateBuffer.readerIndex(msgStartIndex);
            throw new IllegalStateException("Non CommandComplete message.");
        }
        final int nextMsgIndex = msgStartIndex + 1 + cumulateBuffer.readInt();
        final boolean moreResult = status == ResultSetStatus.MORE_RESULT;
        final String commandTag = Messages.readString(cumulateBuffer, clientCharset);
        cumulateBuffer.readerIndex(nextMsgIndex); // avoid tail filler

        final PgResultStates state;
        final int resultIndex = getAndIncrementResultIndex();
        if (cumulateBuffer.getInt(nextMsgIndex) == Messages.N) {
            // next is warning NoticeResponse
            NoticeMessage nm = NoticeMessage.read(cumulateBuffer, clientCharset);
            state = PgResultStates.create(resultIndex, moreResult, commandTag, nm);
        } else {
            state = PgResultStates.create(resultIndex, moreResult, commandTag);
        }
        this.sink.next(state);
        return true;
    }


    private void assertPhase(Phase expected) {
        if (this.phase != expected) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't expected[%s]", this.phase, expected));
        }
    }

    /*################################## blow private instance class ##################################*/

    private enum Phase {
        READ_COMMAND_RESPONSE,
        READ_ROW_SET,
        END
    }


}
