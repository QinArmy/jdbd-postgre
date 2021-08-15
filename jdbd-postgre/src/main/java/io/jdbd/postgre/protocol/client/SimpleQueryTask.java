package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.Encoding;
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
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.function.Consumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class SimpleQueryTask extends PgTask {

    /*################################## blow for static stmt ##################################*/

    /**
     * <p>
     * This method is underlying api of {@link StaticStatement#executeUpdate(String)} method.
     * </p>
     */
    static Mono<ResultState> update(Stmt stmt, TaskAdjutant adjutant) {
        return MultiResults.update(adjutant, sink -> {
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
        return MultiResults.query(adjutant, stmt.getStatusConsumer(), sink -> {
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
        return MultiResults.batchUpdate(adjutant, sink -> {
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
        return MultiResults.asFlux(adjutant, sink -> {
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
        return MultiResults.update(adjutant, sink -> {
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
        return MultiResults.query(adjutant, stmt.getStatusConsumer(), sink -> {
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
        return MultiResults.batchUpdate(adjutant, sink -> {
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
        return MultiResults.asFlux(adjutant, sink -> {
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
        return MultiResults.asFlux(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(adjutant, stmt, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }


    private final DownstreamSink downstreamSink;

    private Phase phase;

    /**
     * <p>
     * create instance for single static statement.
     * </p>
     *
     * @see #update(Stmt, TaskAdjutant)
     * @see #query(Stmt, TaskAdjutant)
     */
    private SimpleQueryTask(Stmt stmt, FluxResultSink sink, TaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createStaticSingleCommand(stmt, adjutant);
        this.downstreamSink = new MultiResultDownstreamSink(this, sink);
    }

    /**
     * @see #batchUpdate(GroupStmt, TaskAdjutant)
     * @see #staticAsMulti(GroupStmt, TaskAdjutant)
     * @see #staticAsFlux(GroupStmt, TaskAdjutant)
     */
    private SimpleQueryTask(GroupStmt stmt, FluxResultSink sink, TaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createStaticBatchCommand(stmt, adjutant);
        this.downstreamSink = new MultiResultDownstreamSink(this, sink);
    }

    /*################################## blow for bindable single stmt ##################################*/

    /**
     * @see #bindableUpdate(BindableStmt, TaskAdjutant)
     * @see #bindableQuery(BindableStmt, TaskAdjutant)
     */
    private SimpleQueryTask(FluxResultSink sink, BindableStmt stmt, TaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createBindableSingleCommand(stmt, adjutant);
        this.downstreamSink = new MultiResultDownstreamSink(this, sink);
    }

    /**
     * @see #bindableBatchUpdate(BatchBindStmt, TaskAdjutant)
     * @see #bindableAsMulti(BatchBindStmt, TaskAdjutant)
     * @see #bindableAsFlux(BatchBindStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, FluxResultSink sink, BatchBindStmt stmt) {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createBindableMultiCommand(stmt, adjutant);
        this.downstreamSink = new MultiResultDownstreamSink(this, sink);
    }

    /*################################## blow for bindable multi stmt ##################################*/

    /**
     * @see #multiStmtAsMulti(MultiBindStmt, TaskAdjutant)
     * @see #multiStmtAsFlux(MultiBindStmt, TaskAdjutant)
     */
    private SimpleQueryTask(TaskAdjutant adjutant, MultiBindStmt stmt, FluxResultSink sink) {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createMultiStmtCommand(stmt, adjutant);
        this.downstreamSink = new MultiResultDownstreamSink(this, sink);
    }


    @Override
    protected final Publisher<ByteBuf> start() {
        final Publisher<ByteBuf> publisher = this.packetPublisher;
        if (publisher == null) {
            this.phase = Phase.END;
            this.downstreamSink.error(new PgJdbdException("No found command message publisher."));
        } else {
            this.phase = Phase.READ_COMMAND_RESPONSE;
            this.packetPublisher = null;
        }
        return publisher;
    }

    @Override
    protected final boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final Charset clientCharset = this.adjutant.clientCharset();
        boolean taskEnd = false, continueDecode = true;
        while (continueDecode) {
            final int msgType = cumulateBuffer.readByte(), bodyIndex = cumulateBuffer.readerIndex();
            final int length = cumulateBuffer.readInt(), nextMsgIndex = bodyIndex + length - 4;

            switch (msgType) {
                case Messages.E: {// ErrorResponse message
                    ErrorMessage error = ErrorMessage.readBody(cumulateBuffer, nextMsgIndex, clientCharset);
                    addError(PgExceptions.createErrorException(error));
                }
                break;
                case Messages.Z: {// ReadyForQuery message
                    taskEnd = true;
                    serverStatusConsumer.accept(TxStatus.from(cumulateBuffer.readByte()));
                }
                break;
                case Messages.I: {// EmptyQueryResponse message
                    this.downstreamSink.nextUpdate(PgResultState.EMPTY_STATE);
                }
                break;
                case Messages.C: {// CommandComplete message
                    final String commandTag = Messages.readCommandComplete(cumulateBuffer, clientCharset);
                    final PgResultState state;
                    if (cumulateBuffer.getInt(nextMsgIndex) == Messages.N) {
                        // next is NoticeResponse
                        cumulateBuffer.readByte();//skip message type byte
                        NoticeMessage nm = NoticeMessage.readBody(cumulateBuffer, Encoding.CLIENT_CHARSET);
                        serverStatusConsumer.accept(nm);
                        state = PgResultState.create(commandTag, nm);
                    } else {
                        state = PgResultState.create(commandTag);
                    }
                    this.downstreamSink.nextUpdate(state);
                }
                break;
                case Messages.T: {// RowDescription message

                }
                break;
                case Messages.N: {// NoticeResponse message

                }
                default: {

                }


            } //  switch (msgType)

            if (msgType != Messages.C) {
                cumulateBuffer.readerIndex(nextMsgIndex); // avoid tail filler
            }
            if (continueDecode) {
                continueDecode = Messages.hasOneMessage(cumulateBuffer);
            }
        }

        return taskEnd;
    }

    @Override
    protected final boolean canDecode(ByteBuf cumulateBuffer) {
        if (!Messages.hasOneMessage(cumulateBuffer)) {
            return false;
        }
        final int originalIndex = cumulateBuffer.readerIndex();
        boolean canDecode = false;
        if (cumulateBuffer.getByte(originalIndex) == Messages.C) {
            Messages.skipOneMessage(cumulateBuffer);
            if (Messages.hasOneMessage(cumulateBuffer)) {
                switch (cumulateBuffer.readByte()) {
                    case Messages.N:// NoticeResponse
                    case Messages.T:// RowDescription
                    case Messages.Z:// ReadyForQuery
                        canDecode = true;
                        break;
                }
            }
            cumulateBuffer.readerIndex(originalIndex);
        }
        return canDecode;
    }

    @Override
    protected final Action onError(Throwable e) {
        return null;
    }

    private boolean readCommandResponse(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        assertPhase(Phase.READ_COMMAND_RESPONSE);
        return false;
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

    private interface DownstreamSink {

        void error(JdbdException error);

        void nextUpdate(ResultState resultState);
    }

    private static abstract class AbstractDownstreamSink implements DownstreamSink {


    }


    private static final class MultiResultDownstreamSink implements DownstreamSink {

        private final SimpleQueryTask task;

        private final FluxResultSink sink;

        private MultiResultDownstreamSink(SimpleQueryTask task, FluxResultSink sink) {
            this.task = task;
            this.sink = sink;
        }

        @Override
        public final void error(JdbdException error) {
            this.sink.error(error);
        }

        @Override
        public final void nextUpdate(ResultState resultState) {

        }


    }


}
