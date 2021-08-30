package io.jdbd.postgre.protocol.client;

import io.jdbd.SessionCloseException;
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
import io.jdbd.vendor.stmt.StaticStmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.function.Consumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class SimpleQueryTask extends AbstractStmtTask {

    /*################################## blow for static stmt ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     */
    static Mono<ResultState> update(StaticStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.update(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeQuery(String)} method.
     * </p>
     */
    static Flux<ResultRow> query(StaticStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.query(stmt.getStatusConsumer(), sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsMulti(java.util.List)} method.
     * </p>
     */
    static MultiResult batchAsMulti(GroupStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asMulti(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(java.util.List)} method.
     * </p>
     */
    static Flux<Result> batchAsFlux(GroupStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeAsFlux(String)} method.
     * </p>
     */
    static Flux<Result> multiCommandAsFlux(StaticStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
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
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private final FluxResultSink sink;

    private final ResultSetReader resultSetReader;

    private Phase phase;

    /**
     * <p>
     * create instance for single static statement.
     * </p>
     *
     * @see #update(StaticStmt, TaskAdjutant)
     * @see #query(StaticStmt, TaskAdjutant)
     * @see #multiCommandAsFlux(StaticStmt, TaskAdjutant)
     */
    private SimpleQueryTask(StaticStmt stmt, FluxResultSink sink, TaskAdjutant adjutant) throws Throwable {
        super(adjutant, sink, stmt);
        this.packetPublisher = QueryCommandWriter.createStaticCommand(stmt.getSql(), adjutant);
        this.sink = sink;
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /**
     * @see #batchUpdate(GroupStmt, TaskAdjutant)
     * @see #batchAsMulti(GroupStmt, TaskAdjutant)
     * @see #batchAsFlux(GroupStmt, TaskAdjutant)
     */
    private SimpleQueryTask(GroupStmt stmt, FluxResultSink sink, TaskAdjutant adjutant)
            throws Throwable {
        super(adjutant, sink, stmt);
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
        super(adjutant, sink, stmt);
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
        super(adjutant, sink, stmt);
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
        super(adjutant, sink, stmt);
        this.packetPublisher = QueryCommandWriter.createMultiStmtCommand(stmt, adjutant);
        this.sink = sink;
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }


    /*################################## blow io.jdbd.postgre.protocol.client.StmtTask method ##################################*/


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
        boolean taskEnd = false, continueRead = Messages.hasOneMessage(cumulateBuffer);
        while (continueRead) {
            switch (this.phase) {
                case READ_COMMAND_RESPONSE: {
                    taskEnd = readCommandResponse(cumulateBuffer, serverStatusConsumer);
                    continueRead = false;
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
                    throw PgExceptions.createUnexpectedEnumException(this.phase);
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
        if (this.phase != Phase.END) {
            addError(new SessionCloseException("Unexpected session close."));
            publishError(this.sink::error);
        }
    }

    @Override
    protected final Action onError(Throwable e) {
        if (this.phase != Phase.END) {
            addError(e);
            publishError(this.sink::error);
        }
        return Action.TASK_END;
    }

    @Override
    protected final boolean skipPacketsOnError(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        cumulateBuffer.readerIndex(cumulateBuffer.writerIndex());
        return true;
    }

    @Override
    final void internalToString(StringBuilder builder) {
        builder.append(",phase:")
                .append(this.phase);
    }


    /**
     * @return true: task end.
     * @see #decode(ByteBuf, Consumer)
     */
    private boolean readCommandResponse(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_COMMAND_RESPONSE);

        final Charset clientCharset = this.adjutant.clientCharset();

        boolean taskEnd = false, continueRead = true;
        while (continueRead) {
            final int msgStartIndex = cumulateBuffer.readerIndex();
            final int msgType = cumulateBuffer.getByte(msgStartIndex);

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
                    log.trace("Simple query command end,read optional notice.");
                    readNoticeAfterReadyForQuery(cumulateBuffer, serverStatusConsumer);
                }
                break;
                case Messages.I: {// EmptyQueryResponse message
                    continueRead = readEmptyQuery(cumulateBuffer) && Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.S: {// ParameterStatus message
                    serverStatusConsumer.accept(Messages.readParameterStatus(cumulateBuffer, clientCharset));
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.C: {// CommandComplete message
                    if (readResultStateWithoutReturning(cumulateBuffer)) {
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }

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
                    log.debug("Receive NoticeMessage that don't follow CommandComplete. message:\n{}", noticeMessage);
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                case Messages.G: {// CopyInResponse message
                    handleCopyInResponse(cumulateBuffer);
                    continueRead = false;
                }
                break;
                case Messages.H: { // CopyOutResponse message
                    if (handleCopyOutResponse(cumulateBuffer)) {
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case Messages.d: // CopyData message
                case Messages.c:// CopyDone message
                case Messages.f: {// CopyFail message
                    if (handleCopyOutData(cumulateBuffer)) {
                        continueRead = Messages.hasOneMessage(cumulateBuffer);
                    } else {
                        continueRead = false;
                    }
                }
                break;
                case Messages.A: { // NotificationResponse
                    //TODO complete LISTEN command
                    Messages.skipOneMessage(cumulateBuffer);
                    continueRead = Messages.hasOneMessage(cumulateBuffer);
                }
                break;
                default: {
                    throw new UnExpectedMessageException(String.format("Server response unknown message type[%s]"
                            , (char) msgType));
                }


            } //  switch (msgType)

        }

        return taskEnd;
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
