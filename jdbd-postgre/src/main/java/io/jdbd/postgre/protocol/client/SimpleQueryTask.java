package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.session.SessionCloseException;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.result.ResultSink;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * The class is implementation of postgre simple query protocol.
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
 * </p>
 *
 * @see QueryCommandWriter
 * @see ExtendedQueryTask
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4">Simple Query</a>
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Simple Query Message Formats</a>
 */
final class SimpleQueryTask extends PgCommandTask implements SimpleStmtTask {

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
    static Flux<ResultRow> query(StaticStmt stmt, Function<CurrentRow, ResultRow> func, TaskAdjutant adjutant) {
        return MultiResults.query(func, stmt.getStatusConsumer(), sink -> {
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
     * This method is underlying api of {@link StaticStatement#executeBatchUpdate(java.util.List)} method.
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


    static BatchQuery batchQuery(final StaticBatchStmt stmt, final TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
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
    static OrderedFlux executeAsFlux(StaticStmt stmt, TaskAdjutant adjutant) {
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
    static Mono<ResultStates> paramUpdate(ParamStmt stmt, TaskAdjutant adjutant) {
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
     * This method is one of underlying api of below methods:
     * <ul>
     *     <li>{@link BindStatement#executeQuery()}</li>
     *     <li>{@link BindStatement#executeQuery(Function, Consumer)}</li>
     * </ul>
     * </p>
     */
    static Flux<ResultRow> paramQuery(ParamStmt stmt, Function<CurrentRow, ResultRow> func, TaskAdjutant adjutant) {
        return MultiResults.query(func, stmt.getStatusConsumer(), sink -> {
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
     * This method is one of underlying api of {@link BindStatement#executeBatchUpdate()} method.
     * </p>
     */
    static Flux<ResultStates> paramBatchUpdate(ParamBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static BatchQuery paramBatchQuery(ParamBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
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
    static MultiResult paramBatchAsMulti(ParamBatchStmt stmt, TaskAdjutant adjutant) {
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
     * This method is one of underlying api of below methods {@link BindStatement#executeBatchAsFlux()}.
     * </p>
     */
    static OrderedFlux paramBatchAsFlux(ParamBatchStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }



    /*################################## blow for multi stmt  ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link MultiStatement#executeBatchUpdate()} method.
     * </p>
     */
    static Flux<ResultStates> multiStmtBatchUpdate(ParamMultiStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchUpdate(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    static BatchQuery multiStmtBatchQuery(ParamMultiStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.batchQuery(adjutant, sink -> {
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
     * This method is underlying api of {@link MultiStatement#executeBatchAsMulti()} method.
     * </p>
     */
    static MultiResult multiStmtAsMulti(ParamMultiStmt stmt, TaskAdjutant adjutant) {
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
     * This method is underlying api of {@link MultiStatement#executeBatchAsFlux()} method.
     * </p>
     */
    static OrderedFlux multiStmtAsFlux(ParamMultiStmt stmt, TaskAdjutant adjutant) {
        return MultiResults.asFlux(sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }

    private Phase phase;


    private SimpleQueryTask(Stmt stmt, ResultSink sink, TaskAdjutant adjutant) throws JdbdException {
        super(adjutant, sink, stmt);

        if (stmt instanceof StaticStmt) {
            this.packetPublisher = QueryCommandWriter.staticCommand(((StaticStmt) stmt).getSql(), adjutant);
        } else if (stmt instanceof StaticMultiStmt) {
            this.packetPublisher = QueryCommandWriter.staticCommand(((StaticMultiStmt) stmt).getMultiStmt(), adjutant);
        } else if (stmt instanceof StaticBatchStmt) {
            this.packetPublisher = QueryCommandWriter.staticBatchCommand((StaticBatchStmt) stmt, adjutant);
        } else if (stmt instanceof ParamStmt) {
            this.packetPublisher = QueryCommandWriter.paramCommand((ParamStmt) stmt, adjutant);
        } else if (stmt instanceof ParamBatchStmt) {
            this.packetPublisher = QueryCommandWriter.paramBatchCommand((ParamBatchStmt) stmt, adjutant);
        } else if (stmt instanceof ParamMultiStmt) {
            this.packetPublisher = QueryCommandWriter.multiStmtCommand((ParamMultiStmt) stmt, adjutant);
        } else {
            throw PgExceptions.unknownStmt(stmt);
        }

    }

    /*################################## blow for bindable multi stmt ##################################*/


    /*#################### blow io.jdbd.postgre.protocol.client.SimpleStmtTask method ##########################*/

    @Override
    public final void handleNoQueryMessage() {
        // create Query message occur error,end task.
        this.sendPacketSignal(true)
                .subscribe();
    }


    /*############################### blow io.jdbd.postgre.protocol.client.StmtTask method ########################*/


    @Override
    protected Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher == null) {
            this.phase = Phase.END;
            this.sink.error(new JdbdException("No found command message publisher."));
        } else {
            this.phase = Phase.READ_COMMAND_RESPONSE;
            this.packetPublisher = null;
        }
        return publisher;
    }


    @Override
    protected void onChannelClose() {
        if (this.phase != Phase.END) {
            addError(new SessionCloseException("Unexpected session close."));
            publishError(this.sink::error);
        }
    }

    @Override
    protected Action onError(Throwable e) {
        if (this.phase != Phase.END) {
            addError(e);
            publishError(this.sink::error);
        }
        return Action.TASK_END;
    }


    @Override
    protected boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
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
