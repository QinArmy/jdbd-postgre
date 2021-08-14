package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.Encoding;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.result.JdbdMultiResults;
import io.jdbd.vendor.result.MultiResultSink;
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


    static Mono<ResultState> update(Stmt stmt, TaskAdjutant adjutant) {
        return JdbdMultiResults.update_0(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }

    static Flux<ResultRow> query(Stmt stmt, TaskAdjutant adjutant) {
        return JdbdMultiResults.query(adjutant, stmt.getStatusConsumer(), sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(PgExceptions.wrap(e));
            }
        });
    }


    static Flux<SingleResult> asFlux(GroupStmt stmt, TaskAdjutant adjutant) {
        return JdbdMultiResults.createAsFlux(adjutant, sink -> {
            try {
                SimpleQueryTask task = new SimpleQueryTask(stmt, sink, adjutant);
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
    private SimpleQueryTask(Stmt stmt, MultiResultSink sink, TaskAdjutant adjutant) throws SQLException {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createStaticSingleCommand(stmt, adjutant);
        this.downstreamSink = new MultiResultDownstreamSink(this, sink);
    }

    private SimpleQueryTask(GroupStmt stmt, MultiResultSink sink, TaskAdjutant adjutant)
            throws SQLException {
        super(adjutant);
        this.packetPublisher = QueryCommandWriter.createStaticBatchCommand(stmt, adjutant);
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

        private final MultiResultSink sink;

        private MultiResultDownstreamSink(SimpleQueryTask task, MultiResultSink sink) {
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
