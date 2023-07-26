package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.session.Isolation;
import io.jdbd.session.ServerVersion;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.TransactionStatus;
import io.jdbd.vendor.session.JdbdTransactionStatus;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
final class ClientProtocol implements MySQLProtocol {


    static ClientProtocol create(ProtocolManager manager) {
        return new ClientProtocol(manager);

    }

    private final ProtocolManager manager;

    private final TaskAdjutant adjutant;


    private ClientProtocol(final ProtocolManager manager) {
        this.manager = manager;
        this.adjutant = manager.adjutant();
    }


    /*################################## blow ClientCommandProtocol method ##################################*/


    @Override
    public long getId() {
        return this.adjutant.handshake10().threadId;
    }


    @Override
    public Mono<ResultStates> update(StaticStmt stmt) {
        return ComQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public <R> Flux<R> query(StaticStmt stmt, Function<CurrentRow, R> function) {
        return ComQueryTask.query(stmt, function, this.adjutant);
    }

    @Override
    public Flux<ResultStates> batchUpdate(StaticBatchStmt stmt) {
        return ComQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery batchQuery(StaticBatchStmt stmt) {
        return ComQueryTask.batchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult batchAsMulti(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux batchAsFlux(final StaticBatchStmt stmt) {
        return ComQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux executeAsFlux(final StaticMultiStmt stmt) {
        return ComQueryTask.executeAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<ResultStates> bindUpdate(ParamStmt stmt, boolean usePrepare) {
        final Mono<ResultStates> mono;
        if (usePrepare) {
            mono = ComPreparedTask.update(stmt, this.adjutant);
        } else {
            mono = ComQueryTask.paramUpdate(stmt, this.adjutant);
        }
        return mono;
    }

    @Override
    public <R> Flux<R> bindQuery(ParamStmt stmt, boolean usePrepare, Function<CurrentRow, R> function) {
        final Flux<R> flux;
        if (usePrepare || stmt.getFetchSize() > 0) {
            flux = ComPreparedTask.query(stmt, function, this.adjutant);
        } else {
            flux = ComQueryTask.paramQuery(stmt, function, this.adjutant);
        }
        return flux;
    }


    @Override
    public Flux<ResultStates> bindBatchUpdate(ParamBatchStmt stmt, boolean usePrepare) {
        final Flux<ResultStates> flux;
        if (usePrepare) {
            flux = ComPreparedTask.batchUpdate(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchUpdate(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public BatchQuery bindBatchQuery(ParamBatchStmt stmt, boolean usePrepare) {
        final BatchQuery batchQuery;
        if (usePrepare) {
            batchQuery = ComPreparedTask.batchQuery(stmt, this.adjutant);
        } else {
            batchQuery = ComQueryTask.paramBatchQuery(stmt, this.adjutant);
        }
        return batchQuery;
    }

    @Override
    public MultiResult bindBatchAsMulti(final ParamBatchStmt stmt, final boolean usePrepare) {
        final MultiResult result;
        if (usePrepare) {
            result = ComPreparedTask.batchAsMulti(stmt, this.adjutant);
        } else {
            result = ComQueryTask.paramBatchAsMulti(stmt, this.adjutant);
        }
        return result;
    }

    @Override
    public OrderedFlux bindBatchAsFlux(final ParamBatchStmt stmt, final boolean usePrepare) {
        final OrderedFlux flux;
        if (usePrepare) {
            flux = ComPreparedTask.batchAsFlux(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchAsFlux(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public Flux<ResultStates> multiStmtBatchUpdate(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery multiStmtBatchQuery(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtBatchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult multiStmtAsMulti(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux multiStmtAsFlux(ParamMultiStmt stmt) {
        return ComQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<PrepareTask> prepare(String sql) {
        return ComPreparedTask.prepare(sql, this.adjutant);
    }


    @Override
    public Mono<Void> reconnect() {
        return this.manager.reConnect();
    }

    @Override
    public boolean supportOutParameter() {
        return this.adjutant.handshake10().serverVersion.isSupportOutParameter();
    }

    @Override
    public boolean supportSavePoints() {
        return true;
    }

    @Override
    public boolean supportStmtVar() {
        return this.adjutant.handshake10().serverVersion.isSupportQueryAttr();
    }


    @Override
    public boolean inTransaction() {
        return Terminator.inTransaction(this.adjutant.getServerStatus());
    }

    @Override
    public Mono<TransactionStatus> transactionStatus() {
        final MySQLServerVersion version = this.adjutant.handshake10().serverVersion;
        final StringBuilder builder = new StringBuilder(139);
        if (version.meetsMinimum(8, 0, 3)
                || (version.meetsMinimum(5, 7, 20) && !version.meetsMinimum(8, 0, 0))) {
            builder.append("SELECT @@session.transaction_isolation AS txLevel")
                    .append(",@@session.transaction_read_only AS txReadOnly");
        } else {
            builder.append("SELECT @@session.tx_isolation AS txLevel")
                    .append(",@@session.tx_read_only AS txReadOnly");
        }

        final ResultStates[] statesHolder = new ResultStates[1];

        final StaticStmt stmt;
        stmt = Stmts.stmt(builder.toString(), states -> statesHolder[0] = states);
        return ComQueryTask.query(stmt, CurrentRow::asResultRow, this.adjutant)
                .last() // must wait for last ,because statesHolder
                .map(row -> mapTxStatus(row, statesHolder[0]));
    }


    @Override
    public Mono<Void> startTransaction(final TransactionOption option) {

        final StringBuilder builder = new StringBuilder(50);
        final Isolation isolation = option.getIsolation();
        if (isolation != null) {
            builder.append("SET TRANSACTION ISOLATION LEVEL ");
            if (appendIsolation(isolation, builder)) {
                return Mono.error(MySQLExceptions.unknownIsolation(isolation));
            }
            builder.append(" ; ");
        }

        if (option.isReadOnly()) {
            builder.append("START TRANSACTION READ ONLY");
        } else {
            builder.append("START TRANSACTION READ WRITE");
        }
        return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(builder.toString()), this.adjutant))
                .last()
                .map(ResultStates.class::cast)
                .flatMap(this::handleStartTransactionEnd);
    }

    @Override
    public Mono<Void> commit() {
        final String sql = "COMMIT ; SET @@session.autocommit = 1";
        return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(sql), this.adjutant))
                .last()
                .map(ResultStates.class::cast)
                .flatMap(this::handleTransactionEnd);
    }

    @Override
    public Mono<Void> rollback() {
        final String sql = "ROLLBACK ; SET @@session.autocommit = 1";
        return Flux.from(ComQueryTask.executeAsFlux(Stmts.multiStmt(sql), this.adjutant))
                .last()
                .map(ResultStates.class::cast)
                .flatMap(this::handleTransactionEnd);
    }


    @Override
    public Mono<Void> close() {
        return QuitTask.quit(this.adjutant);
    }


    @Override
    public Mono<Void> reset() {
        return Mono.defer(this.manager::reset);
    }

    @Override
    public Mono<Void> ping(final int timeSeconds) {
        return PingTask.ping(timeSeconds, this.adjutant);
    }


    @Override
    public boolean supportMultiStmt() {
        return Capabilities.supportMultiStatement(this.adjutant.capability());
    }

    @Override
    public ServerVersion serverVersion() {
        return this.adjutant.handshake10().serverVersion;
    }

    @Override
    public boolean isClosed() {
        return !this.adjutant.isActive();
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #startTransaction(TransactionOption)
     */
    private Mono<Void> handleStartTransactionEnd(final ResultStates states) {
        final Mono<Void> mono;
        if (states.inTransaction()) {
            mono = Mono.empty();
        } else {
            //no bug,never here
            mono = Mono.error(new JdbdException("start transaction failure."));
        }
        return mono;
    }


    private boolean appendIsolation(final Isolation isolation, final StringBuilder builder) {

        boolean error = false;
        if (isolation == Isolation.READ_COMMITTED) {
            builder.append("READ COMMITTED");
        } else if (isolation == Isolation.REPEATABLE_READ) {
            builder.append("REPEATABLE READ");
        } else if (isolation == Isolation.SERIALIZABLE) {
            builder.append("SERIALIZABLE");
        } else if (isolation == Isolation.READ_UNCOMMITTED) {
            builder.append("READ UNCOMMITTED");
        } else {
            error = true;
        }
        return error;
    }


    /**
     * @see #transactionStatus()
     */
    private TransactionStatus mapTxStatus(final ResultRow row, final ResultStates states) {
        Objects.requireNonNull(states, "states");

        final String txLevel;
        txLevel = row.getNonNull("txLevel", String.class);

        final Isolation isolation;
        if (txLevel.equalsIgnoreCase("READ-COMMITTED")) {
            isolation = Isolation.READ_COMMITTED;
        } else if (txLevel.equalsIgnoreCase("REPEATABLE-READ")) {
            isolation = Isolation.REPEATABLE_READ;
        } else if (txLevel.equalsIgnoreCase("SERIALIZABLE")) {
            isolation = Isolation.SERIALIZABLE;
        } else if (txLevel.equalsIgnoreCase("READ-UNCOMMITTED")) {
            isolation = Isolation.READ_UNCOMMITTED;
        } else {
            final String m;
            m = String.format("transaction_isolation[%s] couldn't map to %s", txLevel, Isolation.class.getName());
            throw new JdbdException(m);
        }
        return JdbdTransactionStatus.txStatus(isolation, row.getNonNull("txReadOnly", Boolean.class), states.inTransaction());
    }


    private Mono<Void> handleTransactionEnd(final ResultStates states) {
        final Mono<Void> mono;
        if (states.inTransaction()) {
            // no bug,never here
            mono = Mono.error(new JdbdException("end transaction failure"));
        } else {
            mono = Mono.empty();
        }
        return mono;
    }


}
