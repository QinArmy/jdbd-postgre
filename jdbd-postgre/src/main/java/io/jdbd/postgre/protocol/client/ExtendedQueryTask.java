package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.session.PgDatabaseSession;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.MultiResult;
import io.jdbd.result.Result;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
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

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY"> Extended Query</a>
 */
final class ExtendedQueryTask extends AbstractStmtTask {


    static Mono<ResultState> update(BindStmt stmt, TaskAdjutant adjutant) {
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

    static Flux<ResultState> batchUpdate(BatchBindStmt stmt, TaskAdjutant adjutant) {
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

    static Mono<PreparedStatement> prepare(String sql, PgDatabaseSession session, TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            PrepareFluxResultSink resultSink = new PrepareFluxResultSink(PgPrepareStmt.prepare(sql), session, sink);
            ExtendedQueryTask task = new ExtendedQueryTask(adjutant, resultSink);
            task.submit(sink::error);
        });
    }


    /*################################## blow Constructor method ##################################*/


    private Phase phase;

    /**
     * @see #update(BindStmt, TaskAdjutant)
     * @see #query(BindStmt, TaskAdjutant)
     */
    private ExtendedQueryTask(BindStmt stmt, FluxResultSink sink, TaskAdjutant adjutant) {
        super(adjutant, sink, stmt);
    }

    /**
     * @see #batchUpdate(BatchBindStmt, TaskAdjutant)
     * @see #batchAsMulti(BatchBindStmt, TaskAdjutant)
     * @see #batchAsFlux(BatchBindStmt, TaskAdjutant)
     */
    private ExtendedQueryTask(FluxResultSink sink, BatchBindStmt stmt, TaskAdjutant adjutant) {
        super(adjutant, sink, stmt);
    }


    /**
     * @see #prepare(String, PgDatabaseSession, TaskAdjutant)
     */
    private ExtendedQueryTask(TaskAdjutant adjutant, PrepareFluxResultSink sink) {
        super(adjutant, sink, sink.prepareStmt);
    }


    @Override
    protected final Publisher<ByteBuf> start() {

        Publisher<ByteBuf> publisher;
        try {
            final Stmt stmt = this.stmt;
            final PgStatement statement;
            if (stmt instanceof BindStmt) {
                statement = this.adjutant.sqlParser().parse(((BindStmt) stmt).getSql());
            } else if (stmt instanceof BatchBindStmt) {
                statement = this.adjutant.sqlParser().parse(((BatchBindStmt) stmt).getSql());
            } else {
                String m = String.format("Unexpected %s type[%s]", Stmt.class.getName(), stmt.getClass().getName());
                throw new IllegalStateException(m);
            }
            publisher = sendStartSeriesMessage(statement);
        } catch (Throwable e) {
            publisher = null;
            this.phase = Phase.END;
        }
        return null;
    }

    @Override
    protected final boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        return false;
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


    private Publisher<ByteBuf> sendStartSeriesMessage(PgStatement statement) {
        return Mono.empty();
    }


    enum Phase {
        READ_PARSE_RESPONSE,
        END
    }


    private static final class PrepareFluxResultSink implements FluxResultSink {

        private final PgPrepareStmt prepareStmt;

        private final PgDatabaseSession session;

        private final MonoSink<PreparedStatement> stmtSink;

        private FluxResultSink resultSink;

        private PrepareFluxResultSink(PgPrepareStmt prepareStmt, PgDatabaseSession session
                , MonoSink<PreparedStatement> sink) {
            this.prepareStmt = prepareStmt;
            this.session = session;
            this.stmtSink = sink;
        }

        private void setResultSink(final FluxResultSink resultSink) {
            if (this.resultSink != null) {
                throw new IllegalStateException("this.resultSink isn't null");
            }
            this.resultSink = resultSink;

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
        public final Function<String, Publisher<byte[]>> getImportFunction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            throw new UnsupportedOperationException();
        }


    }//class PgPrepareStmt


}
