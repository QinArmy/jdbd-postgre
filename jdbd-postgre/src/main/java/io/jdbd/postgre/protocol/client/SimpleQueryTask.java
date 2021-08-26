package io.jdbd.postgre.protocol.client;

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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    static Mono<ResultState> update(Stmt stmt, TaskAdjutant adjutant) {
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
    static Flux<ResultRow> query(Stmt stmt, TaskAdjutant adjutant) {
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
    static MultiResult asMulti(GroupStmt stmt, TaskAdjutant adjutant) {
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
    static Flux<Result> asFlux(GroupStmt stmt, TaskAdjutant adjutant) {
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

    /**
     * cache sql list for copy operation response.
     */
    private final List<String> sqlList;

    private final ResultSetReader resultSetReader;


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
        super(adjutant, sink);
        this.packetPublisher = QueryCommandWriter.createStaticSingleCommand(stmt, adjutant);
        this.sink = sink;
        this.sqlList = Collections.singletonList(stmt.getSql());
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /**
     * @see #batchUpdate(GroupStmt, TaskAdjutant)
     * @see #asMulti(GroupStmt, TaskAdjutant)
     * @see #asFlux(GroupStmt, TaskAdjutant)
     */
    private SimpleQueryTask(GroupStmt stmt, FluxResultSink sink, TaskAdjutant adjutant)
            throws Throwable {
        super(adjutant, sink);
        this.packetPublisher = QueryCommandWriter.createStaticBatchCommand(stmt, adjutant);
        this.sink = sink;
        this.sqlList = stmt.getSqlGroup();
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /*################################## blow for bindable single stmt ##################################*/

    /**
     * @see #bindableUpdate(BindableStmt, TaskAdjutant)
     * @see #bindableQuery(BindableStmt, TaskAdjutant)
     */
    private SimpleQueryTask(FluxResultSink sink, BindableStmt stmt, TaskAdjutant adjutant)
            throws Throwable {
        super(adjutant, sink);
        this.packetPublisher = QueryCommandWriter.createBindableCommand(stmt, adjutant);
        this.sink = sink;
        this.sqlList = Collections.singletonList(stmt.getSql());
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /**
     * @see #bindableBatchUpdate(BatchBindStmt, TaskAdjutant)
     * @see #bindableAsMulti(BatchBindStmt, TaskAdjutant)
     * @see #bindableAsFlux(BatchBindStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, FluxResultSink sink, BatchBindStmt stmt)
            throws Throwable {
        super(adjutant, sink);
        this.packetPublisher = QueryCommandWriter.createBindableBatchCommand(stmt, adjutant);
        this.sink = sink;
        this.sqlList = Collections.singletonList(stmt.getSql());
        this.resultSetReader = DefaultResultSetReader.create(this, sink.froResultSet());
    }

    /*################################## blow for bindable multi stmt ##################################*/

    /**
     * @see #multiStmtAsMulti(MultiBindStmt, TaskAdjutant)
     * @see #multiStmtAsFlux(MultiBindStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, MultiBindStmt stmt, FluxResultSink sink)
            throws Throwable {
        super(adjutant, sink);
        this.packetPublisher = QueryCommandWriter.createMultiStmtCommand(stmt, adjutant);
        this.sink = sink;
        this.sqlList = extractSqlList(stmt);
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
        if (!Messages.hasOneMessage(cumulateBuffer)) {
            return false;
        }

        boolean taskEnd = false, continueRead = true;
        while (continueRead) {
            switch (this.phase) {
                case READ_COMMAND_RESPONSE:
                case COPY_IN_MODE:
                case COPY_OUT_MODE:
                case COPY_BOTH_MODE: {
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

    @Override
    protected final boolean skipPacketsOnError(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        cumulateBuffer.readerIndex(cumulateBuffer.writerIndex());
        return true;
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
                }
                break;
                case Messages.C: {// CommandComplete message
                    continueRead = readResultStateWithoutReturning(cumulateBuffer)
                            && Messages.hasOneMessage(cumulateBuffer);
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
                    this.phase = Phase.COPY_IN_MODE;
                    handleCopyInResponse(cumulateBuffer, this.sqlList);
                }
                break;
                case Messages.H: { // CopyOutResponse message
                    this.phase = Phase.COPY_OUT_MODE;
                }
                break;
                case Messages.W: {// CopyBothResponse message
                    this.phase = Phase.COPY_BOTH_MODE;
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


    private void assertPhase(Phase expected) {
        if (this.phase != expected) {
            throw new IllegalStateException(String.format("this.phase[%s] isn't expected[%s]", this.phase, expected));
        }
    }


    private static List<String> extractSqlList(MultiBindStmt stmt) {
        final List<BindableStmt> stmtList = stmt.getStmtGroup();
        final List<String> sqlList = new ArrayList<>(stmtList.size());
        for (BindableStmt bindableStmt : stmtList) {
            sqlList.add(bindableStmt.getSql());
        }
        return Collections.unmodifiableList(sqlList);
    }


    /*################################## blow private instance class ##################################*/

    private enum Phase {
        READ_COMMAND_RESPONSE,
        READ_ROW_SET,
        COPY_IN_MODE,
        COPY_OUT_MODE,
        COPY_BOTH_MODE,
        END
    }


}
