package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.*;
import io.jdbd.session.*;
import io.jdbd.vendor.session.JdbdTransactionStatus;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_command_phase.html">Command Phase</a>
 */
final class ClientProtocol implements MySQLProtocol {


    static ClientProtocol create(SessionManager sessionManager) {
        return new ClientProtocol(sessionManager);

    }

    private final SessionManager sessionManager;

    final TaskAdjutant adjutant;


    private ClientProtocol(final SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        this.adjutant = sessionManager.adjutant();
    }


    /*################################## blow ClientCommandProtocol method ##################################*/


    @Override
    public long getId() {
        return this.adjutant
                .handshake10()
                .getThreadId();
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
    public Mono<ResultStates> bindUpdate(ParamStmt stmt, boolean forcePrepare) {
        final Mono<ResultStates> mono;
        if (forcePrepare) {
            mono = ComPreparedTask.update(stmt, this.adjutant);
        } else {
            mono = ComQueryTask.paramUpdate(stmt, this.adjutant);
        }
        return mono;
    }

    @Override
    public <R> Flux<R> bindQuery(ParamStmt stmt, boolean forcePrepare, Function<CurrentRow, R> function) {
        final Flux<R> flux;
        if (forcePrepare || stmt.getFetchSize() > 0) {
            flux = ComPreparedTask.query(stmt, function, this.adjutant);
        } else {
            flux = ComQueryTask.paramQuery(stmt, function, this.adjutant);
        }
        return flux;
    }


    @Override
    public Flux<ResultStates> bindBatchUpdate(ParamBatchStmt stmt, boolean forcePrepare) {
        final Flux<ResultStates> flux;
        if (forcePrepare) {
            flux = ComPreparedTask.batchUpdate(stmt, this.adjutant);
        } else {
            flux = ComQueryTask.paramBatchUpdate(stmt, this.adjutant);
        }
        return flux;
    }

    @Override
    public BatchQuery bindBatchQuery(ParamBatchStmt stmt, boolean forcePrepare) {
        final BatchQuery batchQuery;
        if (forcePrepare) {
            batchQuery = ComPreparedTask.batchQuery(stmt, this.adjutant);
        } else {
            batchQuery = ComQueryTask.paramBatchQuery(stmt, this.adjutant);
        }
        return batchQuery;
    }

    @Override
    public MultiResult bindBatchAsMulti(final ParamBatchStmt stmt, final boolean forcePrepare) {
        final MultiResult result;
        if (forcePrepare) {
            result = ComPreparedTask.batchAsMulti(stmt, this.adjutant);
        } else {
            result = ComQueryTask.paramBatchAsMulti(stmt, this.adjutant);
        }
        return result;
    }

    @Override
    public OrderedFlux bindBatchAsFlux(final ParamBatchStmt stmt, final boolean forcePrepare) {
        final OrderedFlux flux;
        if (forcePrepare) {
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
    public Mono<Void> reconnect(int maxAttempts) {
        return null;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public boolean supportSavePoints() {
        return false;
    }

    @Override
    public boolean supportStmtVar() {
        return false;
    }

    @Override
    public Mono<SavePoint> createSavePoint() {
        return null;
    }

    @Override
    public Mono<SavePoint> createSavePoint(String name) {
        return null;
    }

    @Override
    public boolean inTransaction() {
        return this.adjutant.inTransaction();
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
        builder.append(",@@session.autocommit AS txAutoCommit");

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        return ComQueryTask.query(Stmts.stmt(builder.toString(), statesHolder::set), this.adjutant)
                .last() // must wait for last ,because statesHolder
                .map(row -> mapTxOption(row, statesHolder.get()));
    }


    @Override
    public Mono<Void> startTransaction(final TransactionOption option) {
        return Mono.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                startTransactionInEventLoop(option, sink);
            } else {
                this.adjutant.execute(() -> startTransactionInEventLoop(option, sink));
            }
        });
    }


    @Override
    public Mono<Void> setTransactionOption(final TransactionOption option) {
        return Mono.create(sink -> {
            if (this.adjutant.inEventLoop()) {
                setTransactionOptionInEventLoop(option, sink);
            } else {
                this.adjutant.execute(() -> setTransactionOptionInEventLoop(option, sink));
            }
        });
    }

    @Override
    public Mono<Void> commit() {
        final List<String> sqlGroup = new ArrayList<>(2);
        sqlGroup.add("COMMIT");
        sqlGroup.add("SET @@session.autocommit = 1");
        return ComQueryTask.batchUpdate(Stmts.batch(sqlGroup), this.adjutant)
                .collectList()
                .flatMap(this::validateCommitResult);
    }

    @Override
    public Mono<Void> rollback() {
        final List<String> sqlGroup = new ArrayList<>(2);
        sqlGroup.add("ROLLBACK");
        sqlGroup.add("SET @@session.autocommit = 1");
        return ComQueryTask.batchUpdate(Stmts.batch(sqlGroup), this.adjutant)
                .collectList()
                .flatMap(this::validateRollbackResult);
    }


    @Override
    public Mono<Void> close() {
        return QuitTask.quit(this.adjutant);
    }


    @Override
    public Mono<Void> reset() {
        return Mono.defer(this.sessionManager::reset)
                .then();
    }

    @Override
    public Mono<Void> ping(final int timeSeconds) {
        return PingTask.ping(timeSeconds, this.adjutant);
    }

    @Override
    public boolean isStartedTransaction(ResultStates states) {
        return TerminatorPacket.startedTransaction(((MySQLResultStates) states).serverStatus);
    }

    @Override
    public boolean isReadOnlyTransaction(ResultStates states) {
        return TerminatorPacket.isReadOnly(((MySQLResultStates) states).serverStatus);
    }

    @Override
    public boolean supportMultiStmt() {
        return Capabilities.supportMultiStatement(this.adjutant.capability());
    }

    @Override
    public ServerVersion serverVersion() {
        return this.adjutant.handshake10().getServerVersion();
    }

    @Override
    public boolean isClosed() {
        return !this.adjutant.isActive();
    }

    /*################################## blow private method ##################################*/


    /**
     * @see #setTransactionOptionInEventLoop(TransactionOption, MonoSink)
     */
    private void setTransactionOptionInEventLoop(final TransactionOption option, final MonoSink<Void> sink) {
        final TaskAdjutant adjutant = this.adjutant;
        if (adjutant.inTransaction()) {
            sink.error(MySQLExceptions.transactionExistsRejectSet(adjutant.handshake10().getThreadId()));
            return;
        }
        final StringBuilder builder = new StringBuilder(70);
        builder.append("SET SESSION TRANSACTION ISOLATION LEVEL ");

        try {
            appendSetIsolation(option.getIsolation(), builder);
        } catch (Throwable e) {
            sink.error(e);
            return;
        }

        if (option.isReadOnly()) {
            builder.append(",READ ONLY");
        } else {
            builder.append(",READ WRITE");
        }
        ComQueryTask.update(Stmts.stmt(builder.toString()), adjutant)
                .subscribe(states -> sink.success(), sink::error);
    }


    /**
     * @see #startTransaction(TransactionOption)
     */
    private void startTransactionInEventLoop(final TransactionOption option, final MonoSink<Void> sink) {
        final TaskAdjutant adjutant = this.adjutant;
        if (adjutant.inTransaction()) {
            sink.error(MySQLExceptions.transactionExistsRejectStart(adjutant.handshake10().getThreadId()));
            return;
        }

        final StringBuilder builder = new StringBuilder(50);
        builder.append("SET TRANSACTION ISOLATION LEVEL ");

        try {
            appendSetIsolation(option.getIsolation(), builder);
        } catch (Throwable e) {
            sink.error(e);
            return;
        }

        final List<String> sqlGroup = new ArrayList<>(2);
        sqlGroup.add(builder.toString());
        if (option.isReadOnly()) {
            sqlGroup.add("START TRANSACTION READ ONLY");
        } else {
            sqlGroup.add("START TRANSACTION READ WRITE");
        }
        ComQueryTask.batchUpdate(Stmts.batch(sqlGroup), adjutant)
                .collectList()
                .subscribe(list -> validateStartTransactionResult(list, sink), sink::error);
    }

    /**
     * @see #setTransactionOptionInEventLoop(TransactionOption, MonoSink)
     * @see #startTransaction(TransactionOption)
     */
    private void appendSetIsolation(final Isolation isolation, final StringBuilder builder) {
        switch (isolation) {
            case READ_COMMITTED:
                builder.append("READ COMMITTED");
                break;
            case REPEATABLE_READ:
                builder.append("REPEATABLE READ");
                break;
            case SERIALIZABLE:
                builder.append("SERIALIZABLE");
                break;
            case READ_UNCOMMITTED:
                builder.append("READ UNCOMMITTED");
                break;
            default:
                throw MySQLExceptions.createUnexpectedEnumException(isolation);
        }

    }

    /**
     * @see #startTransactionInEventLoop(TransactionOption, MonoSink)
     */
    private void validateStartTransactionResult(List<ResultStates> list, MonoSink<Void> sink) {
        if (list.size() != 2) {
            sink.error(new MySQLJdbdException("start transaction failure."));
            return;
        }
        final MySQLResultStates states = (MySQLResultStates) list.get(1);
        if (states.inTransaction()) {
            sink.success();
        } else {
            sink.error(new MySQLJdbdException("start transaction failure."));
        }
    }


    /**
     * @see #transactionStatus()
     */
    private TransactionStatus mapTxOption(final ResultRow row, final ResultStates states) {
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
            throw new MySQLJdbdException(m);
        }

        final boolean readOnly, autoCommit;
        readOnly = MySQLStrings.parseMySqlBoolean("transaction_read_only", row.getNonNull("txReadOnly", String.class));
        autoCommit = MySQLStrings.parseMySqlBoolean("autocommit", row.getNonNull("txAutoCommit", String.class));

        final MySQLResultStates mysqlStates = (MySQLResultStates) states;
        return JdbdTransactionStatus.txStatus(isolation, readOnly, autoCommit || mysqlStates.inTransaction());
    }


    /**
     * @see #commit()
     */
    private Mono<Void> validateCommitResult(List<ResultStates> statesList) {
        final Mono<Void> mono;
        if (statesList.size() != 2) {
            mono = Mono.error(new MySQLJdbdException("COMMIT command execute failure"));
        } else {
            final MySQLResultStates states = (MySQLResultStates) statesList.get(1);
            if (states.inTransaction()) {
                mono = Mono.error(new MySQLJdbdException("COMMIT command execute failure"));
            } else {
                mono = Mono.empty();
            }
        }
        return mono;
    }

    /**
     * @see #rollback()
     */
    private Mono<Void> validateRollbackResult(List<ResultStates> statesList) {
        final Mono<Void> mono;
        if (statesList.size() != 2) {
            mono = Mono.error(new MySQLJdbdException("ROLLBACK command execute failure"));
        } else {
            final MySQLResultStates states = (MySQLResultStates) statesList.get(1);
            if (states.inTransaction()) {
                mono = Mono.error(new MySQLJdbdException("ROLLBACK command execute failure"));
            } else {
                mono = Mono.empty();
            }
        }
        return mono;
    }


    private boolean usePrepare(final BindBatchStmt stmt) {
        boolean prepare = false;
        for (List<BindValue> group : stmt.getGroupList()) {
            if (usePrepare(group)) {
                prepare = true;
                break;
            }
        }
        return prepare;
    }

    private boolean usePrepare(final List<BindValue> group) {
        boolean prepare = false;
        Object value;
        outFor:
        for (BindValue bindValue : group) {
            value = bindValue.get();
            if (value == null) {
                continue;
            }
            if (value instanceof Publisher) {
                prepare = true;
                break;
            }
            switch (bindValue.getType()) {
                case LONGBLOB:
                case LONGTEXT: {
                    if (value instanceof Path) {
                        prepare = true;
                        break outFor;
                    }
                }
                break;
                default:
            }
        }
        return prepare;
    }


}
