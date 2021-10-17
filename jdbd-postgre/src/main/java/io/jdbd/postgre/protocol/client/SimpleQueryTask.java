package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindBatchStmt;
import io.jdbd.postgre.stmt.BindMultiStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.session.SessionCloseException;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4">Simple Query</a>
 */
final class SimpleQueryTask extends AbstractStmtTask implements SimpleStmtTask {

    /*################################## blow for static stmt ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     */
    static Mono<ResultStates> update(StaticStmt stmt, TaskAdjutant adjutant) {
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
    static Flux<ResultStates> batchUpdate(StaticBatchStmt stmt, TaskAdjutant adjutant) {
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
     * This method is underlying api of {@link StaticStatement#executeBatchAsMulti(java.util.List)} method.
     * </p>
     */
    static MultiResult batchAsMulti(StaticBatchStmt stmt, TaskAdjutant adjutant) {
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
     * This method is underlying api of {@link StaticStatement#executeBatchAsFlux(java.util.List)} method.
     * </p>
     */
    static OrderedFlux batchAsFlux(StaticBatchStmt stmt, TaskAdjutant adjutant) {
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
    static OrderedFlux multiCommandAsFlux(StaticStmt stmt, TaskAdjutant adjutant) {
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
     * This method is one of underlying api of {@link BindStatement#executeUpdate()} method.
     * </p>
     */
    static Mono<ResultStates> bindableUpdate(BindStmt stmt, TaskAdjutant adjutant) {
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
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Consumer)}</li>
     * </ul>
     * </p>
     */
    static Flux<ResultRow> bindableQuery(BindStmt stmt, TaskAdjutant adjutant) {
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
     * This method is one of underlying api of {@link BindStatement#executeBatch()} method.
     * </p>
     */
    static Flux<ResultStates> bindableBatchUpdate(BindBatchStmt stmt, TaskAdjutant adjutant) {
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
     * This method is one of underlying api of below methods {@link BindStatement#executeBatchAsMulti()}.
     * </p>
     */
    static MultiResult bindableAsMulti(BindBatchStmt stmt, TaskAdjutant adjutant) {
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
     * This method is one of underlying api of below methods {@link BindStatement#executeBatchAsFlux()}.
     * </p>
     */
    static OrderedFlux bindableAsFlux(BindBatchStmt stmt, TaskAdjutant adjutant) {
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
     * This method is underlying api of {@link MultiStatement#executeBatch()} method.
     * </p>
     */
    static Flux<ResultStates> multiStmtBatch(BindMultiStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
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
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     */
    static MultiResult multiStmtAsMulti(BindMultiStmt stmt, TaskAdjutant adjutant) {
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
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     */
    static OrderedFlux multiStmtAsFlux(BindMultiStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(adjutant, stmt, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private final ResultSink sink;

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
    private SimpleQueryTask(StaticStmt stmt, ResultSink sink, TaskAdjutant adjutant) throws SQLException {
        super(adjutant, sink, stmt);
        this.packetPublisher = QueryCommandWriter.createStaticCommand(stmt.getSql(), adjutant);
        this.sink = sink;
    }

    /**
     * @see #batchUpdate(StaticBatchStmt, TaskAdjutant)
     * @see #batchAsMulti(StaticBatchStmt, TaskAdjutant)
     * @see #batchAsFlux(StaticBatchStmt, TaskAdjutant)
     */
    private SimpleQueryTask(StaticBatchStmt stmt, ResultSink sink, TaskAdjutant adjutant)
            throws Throwable {
        super(adjutant, sink, stmt);
        this.packetPublisher = QueryCommandWriter.createStaticBatchCommand(stmt, adjutant);
        this.sink = sink;
    }

    /*################################## blow for bindable single stmt ##################################*/

    /**
     * @see #bindableUpdate(BindStmt, TaskAdjutant)
     * @see #bindableQuery(BindStmt, TaskAdjutant)
     */
    private SimpleQueryTask(ResultSink sink, BindStmt stmt, TaskAdjutant adjutant) throws Throwable {
        super(adjutant, sink, stmt);
        this.packetPublisher = QueryCommandWriter.createBindableCommand(stmt, adjutant);
        this.sink = sink;
    }

    /**
     * @see #bindableBatchUpdate(BindBatchStmt, TaskAdjutant)
     * @see #bindableAsMulti(BindBatchStmt, TaskAdjutant)
     * @see #bindableAsFlux(BindBatchStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, ResultSink sink, BindBatchStmt stmt) throws Throwable {
        super(adjutant, sink, stmt);
        this.packetPublisher = QueryCommandWriter.createBindableBatchCommand(stmt, adjutant);
        this.sink = sink;
    }

    /*################################## blow for bindable multi stmt ##################################*/

    /**
     * @see #multiStmtAsMulti(BindMultiStmt, TaskAdjutant)
     * @see #multiStmtAsFlux(BindMultiStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, BindMultiStmt stmt, ResultSink sink) throws Throwable {
        super(adjutant, sink, stmt);
        this.packetPublisher = QueryCommandWriter.createMultiStmtCommand(stmt, adjutant);
        this.sink = sink;
    }

    /*#################### blow io.jdbd.postgre.protocol.client.SimpleStmtTask method ##########################*/

    @Override
    public final void handleNoQueryMessage() {
        // create Query message occur error,end task.
        this.sendPacketSignal(true)
                .subscribe();
    }


    /*############################### blow io.jdbd.postgre.protocol.client.StmtTask method ########################*/


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
    protected final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        final boolean taskEnd;
        taskEnd = readExecuteResponse(cumulateBuffer, serverStatusConsumer);
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
    final boolean handlePrepareResponse(List<PgType> paramTypeList, @Nullable ResultRowMeta rowMeta) {
        // never here for simple query
        String msg = String.format("Server response unknown message type[%s]", (char) Messages.t);
        throw new UnExpectedMessageException(msg);
    }

    @Override
    final boolean handleClientTimeout() {
        //TODO
        return false;
    }


    /*################################## blow private instance class ##################################*/

    private enum Phase {
        READ_COMMAND_RESPONSE,
        READ_ROW_SET,
        END
    }


}
