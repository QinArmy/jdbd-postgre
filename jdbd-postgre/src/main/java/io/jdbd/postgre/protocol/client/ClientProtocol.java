package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.*;
import io.jdbd.session.*;
import io.jdbd.vendor.session.JdbdTransactionStatus;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

final class ClientProtocol implements PgProtocol {

    static ClientProtocol create(final ConnectionWrapper wrapper) {
        validateParamMap(wrapper.initializedParamMap);
        return new ClientProtocol(wrapper);
    }

    private static void validateParamMap(Map<String, String> paramMap) {
        if (paramMap.isEmpty()) {
            throw new IllegalArgumentException("Initialized map is empty");
        }
        try {
            paramMap.put("This is a no-exists key,only test.", "");
            throw new IllegalArgumentException("Initialized map isn't unmodified map.");
        } catch (UnsupportedOperationException e) {
            // ok
        }
    }

    private static final String COMMIT = "COMMIT";

    private static final String ROLLBACK = "ROLLBACK";

    private final ProtocolManager protocolManager;
    private final TaskAdjutant adjutant;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Map<String, String> initializedParamMap;

    private ClientProtocol(ConnectionWrapper wrapper) {
        this.protocolManager = wrapper.sessionManager;
        this.adjutant = this.protocolManager.taskAdjutant();
        this.initializedParamMap = wrapper.initializedParamMap;
    }

    @Override
    public long sessionIdentifier() {
        return this.adjutant.processId();
    }

    @Override
    public void bindIdentifier(StringBuilder builder, String identifier) {
        //TODO
    }

    @Override
    public Mono<ResultStates> update(StaticStmt stmt) {
        return SimpleQueryTask.update(stmt, this.adjutant);
    }

    @Override
    public <R> Flux<R> query(StaticStmt stmt, Function<CurrentRow, R> function) {
        return SimpleQueryTask.query(stmt, function, this.adjutant);
    }

    @Override
    public Flux<ResultStates> batchUpdate(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery batchQuery(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult batchAsMulti(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux batchAsFlux(StaticBatchStmt stmt) {
        return SimpleQueryTask.batchAsFlux(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux executeAsFlux(StaticMultiStmt stmt) {
        return SimpleQueryTask.executeAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<ResultStates> paramUpdate(ParamStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.update(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramUpdate(stmt, this.adjutant);
    }

    @Override
    public <R> Flux<R> paramQuery(ParamStmt stmt, boolean usePrepare, Function<CurrentRow, R> function) {
        if (usePrepare) {
            return ExtendedQueryTask.query(stmt, function, this.adjutant);
        }
        return SimpleQueryTask.paramQuery(stmt, function, this.adjutant);
    }

    @Override
    public Flux<ResultStates> paramBatchUpdate(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchUpdate(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery paramBatchQuery(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchQuery(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult paramBatchAsMulti(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchAsMulti(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux paramBatchAsFlux(ParamBatchStmt stmt, boolean usePrepare) {
        if (usePrepare) {
            return ExtendedQueryTask.batchAsFlux(stmt, this.adjutant);
        }
        return SimpleQueryTask.paramBatchAsFlux(stmt, this.adjutant);
    }

    @Override
    public Flux<ResultStates> multiStmtBatchUpdate(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtBatchUpdate(stmt, this.adjutant);
    }

    @Override
    public BatchQuery multiStmtBatchQuery(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtBatchQuery(stmt, this.adjutant);
    }

    @Override
    public MultiResult multiStmtAsMulti(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtAsMulti(stmt, this.adjutant);
    }

    @Override
    public OrderedFlux multiStmtAsFlux(ParamMultiStmt stmt) {
        return SimpleQueryTask.multiStmtAsFlux(stmt, this.adjutant);
    }

    @Override
    public Mono<PrepareTask> prepare(String sql) {
        return ExtendedQueryTask.prepare(sql, this.adjutant);
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-declare.html">define a cursor</a>
     */
    @Override
    public RefCursor refCursor(final String name, final @Nullable Map<Option<?>, ?> optionMap,
                               final DatabaseSession session) {
        if (!PgStrings.hasText(name)) {
            throw new IllegalArgumentException("cursor name must have text.");
        } else if (optionMap == null) {
            throw new NullPointerException("optionMap must be non-null");
        } else if (optionMap.size() > 1 || !optionMap.containsKey(Option.AUTO_CLOSE_ON_ERROR)) {
            final StringBuilder builder = new StringBuilder();
            builder.append("%s don't options[");
            int index = 0;
            for (Option<?> option : optionMap.keySet()) {
                if (option == Option.AUTO_CLOSE_ON_ERROR) {
                    continue;
                }
                if (index > 0) {
                    builder.append(',');
                }
                builder.append(option.name());
                index++;
            }
            builder.append(']');
            throw new JdbdException(builder.toString());
        }

        return PgRefCursor.create(name, PgCursorTask.create(name, optionMap, this.adjutant), session);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-start-transaction.html">START TRANSACTION</a>
     */
    @Override
    public Mono<ResultStates> startTransaction(final @Nullable TransactionOption option, final @Nullable HandleMode mode) {

        final StringBuilder builder = new StringBuilder(50);
        final JdbdException error;

        final Mono<ResultStates> mono;
        if (option == null) {
            mono = Mono.error(new NullPointerException("option must non-null"));
        } else if (mode == null) {
            mono = Mono.error(new NullPointerException("mode must non-null"));
        } else if (this.inTransaction() && (error = handleInTransaction(mode, builder)) != null) {
            mono = Mono.error(error);
        } else {
            builder.append("START TRANSACTION ");
            mono = executeTransactionOption(builder, option);
        }
        return mono;
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-set-transaction.html">SET TRANSACTION</a>
     */
    @Override
    public Mono<ResultStates> setTransactionCharacteristics(final @Nullable TransactionOption option) {
        if (option == null) {
            return Mono.error(new NullPointerException("option must non-null"));
        }
        final StringBuilder builder = new StringBuilder(50);
        builder.append("SET SESSION CHARACTERISTICS AS TRANSACTION ");
        return executeTransactionOption(builder, option);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-set-transaction.html">SET TRANSACTION</a>
     * @see <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-TRANSACTION-ISOLATION">transaction_isolation</a>
     * @see <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-TRANSACTION-READ-ONLY">transaction_read_only</a>
     * @see <a href="https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-TRANSACTION-DEFERRABLE">transaction_deferrable</a>
     */
    @Override
    public Mono<TransactionStatus> transactionStatus() {
        final String sql = "SHOW transaction_isolation ; SHOW transaction_read_only ; SHOW transaction_deferrable ";
        return Flux.from(SimpleQueryTask.executeAsFlux(Stmts.multiStmt(sql), this.adjutant))
                .collectMap(this::transactionStatusKey, this::transactionStatusValue, PgCollections::hashMap)
                .map(this::createTransactionStatus);
    }


    @Override
    public Mono<Void> ping(final int timeSeconds) {
        final String sql = "SELECT 1 AS r";
        return Flux.from(SimpleQueryTask.query(Stmts.stmtWithTimeout(sql, timeSeconds), ROW_FUNC, this.adjutant))
                .then();
    }

    @Override
    public Mono<Void> reset() {
        return this.protocolManager.reset(this.initializedParamMap);
    }

    @Override
    public Mono<Void> reconnect() {
        if (this.closed.get()) {
            return Mono.error(new JdbdException("session invoke close(),reject reconnect"));
        }
        return this.protocolManager.reConnect();
    }

    @Override
    public boolean supportMultiStmt() {
        // true : postgre support multi-statement
        return true;
    }

    @Override
    public boolean supportOutParameter() {
        // true : postgre support out parameter
        return true;
    }

    @Override
    public boolean supportStmtVar() {
        // false : postgre don't support statement-variable.
        return false;
    }

    @Override
    public ServerVersion serverVersion() {
        return this.adjutant.server().serverVersion();
    }


    @Override
    public boolean inTransaction() {
        return this.adjutant.inTransaction();
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-commit.html">COMMIT</a>
     */
    @Override
    public Mono<ResultStates> commit(final Map<Option<?>, ?> optionMap) {
        return executeCommitOrRollback(true, optionMap);
    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-rollback.html">ROLLBACK</a>
     */
    @Override
    public Mono<ResultStates> rollback(final Map<Option<?>, ?> optionMap) {
        return executeCommitOrRollback(false, optionMap);
    }


    @Override
    public boolean isClosed() {
        return !this.adjutant.isActive();
    }

    @Override
    public <T> Mono<T> close() {
        if (!this.adjutant.isActive()) {
            return Mono.empty();
        }
        this.closed.set(true); // here , can't reconnect
        return TerminateTask.terminate(this.adjutant);
    }


    @Override
    public <T> T valueOf(Option<T> option) {
        return null;
    }


    /**
     * @see #commit(Map)
     * @see #rollback(Map)
     */
    private Mono<ResultStates> executeCommitOrRollback(final boolean commit, final Map<Option<?>, ?> optionMap) {
        final StringBuilder builder = new StringBuilder(20);
        if (commit) {
            builder.append(COMMIT);
        } else {
            builder.append(ROLLBACK);
        }
        final Boolean chain = (Boolean) optionMap.get(Option.CHAIN);
        if (chain != null) {
            builder.append(" AND ");
            if (!chain) {
                builder.append("NOT ");
            }
            builder.append("CHAIN");
        }
        return Mono.from(SimpleQueryTask.update(Stmts.stmt(builder.toString()), this.adjutant));
    }


    /**
     * @see #startTransaction(TransactionOption, HandleMode)
     * @see #setTransactionCharacteristics(TransactionOption)
     */
    private Mono<ResultStates> executeTransactionOption(final StringBuilder builder, final TransactionOption option) {
        final Isolation isolation = option.isolation();
        if (isolation != null) {
            builder.append("ISOLATION LEVEL ");
            if (appendIsolation(isolation, builder)) {
                return Mono.error(PgExceptions.unknownIsolation(isolation));
            }
            builder.append(PgConstant.SPACE_COMMA_SPACE);
        }

        if (option.isReadOnly()) {
            builder.append("READ ONLY");
        } else {
            builder.append("READ WRITE");
        }
        final Boolean deferrable = option.valueOf(Option.DEFERRABLE);
        if (deferrable != null) {
            builder.append(PgConstant.SPACE_COMMA_SPACE);
            if (!deferrable) {
                builder.append("NOT ");
            }
            builder.append("DEFERRABLE");
        }
        return Flux.from(SimpleQueryTask.executeAsFlux(Stmts.multiStmt(builder.toString()), this.adjutant))
                .last()
                .map(ResultStates.class::cast);
    }

    /**
     * @see #startTransaction(TransactionOption, HandleMode)
     */
    @Nullable
    private JdbdException handleInTransaction(final HandleMode mode, final StringBuilder builder) {
        JdbdException error = null;
        switch (mode) {
            case ERROR_IF_EXISTS:
                error = PgExceptions.transactionExistsRejectStart(this.sessionIdentifier());
                break;
            case COMMIT_IF_EXISTS:
                builder.append(COMMIT)
                        .append(PgConstant.SPACE_SEMICOLON_SPACE);
                break;
            case ROLLBACK_IF_EXISTS:
                builder.append(ROLLBACK)
                        .append(PgConstant.SPACE_SEMICOLON_SPACE);
                break;
            default:
                error = PgExceptions.unexpectedEnum(mode);

        }
        return error;
    }


    /**
     * @see #transactionStatus()
     */
    @SuppressWarnings("unchecked")
    private TransactionStatus createTransactionStatus(final Map<?, ?> map) {
        map.remove(Option.NAME); // remove Pseudo key
        return JdbdTransactionStatus.fromMap((Map<Option<?>, ?>) map);
    }

    /**
     * @see #transactionStatus()
     */
    private Option<?> transactionStatusKey(final ResultItem item) {
        final Option<?> option;
        if (item instanceof ResultRowMeta) {
            option = Option.NAME; // here ,Pseudo key
        } else if (item instanceof ResultStates) {
            if (((ResultStates) item).hasMoreResult()) {
                option = Option.NAME; // here ,Pseudo key
            } else {
                option = Option.IN_TRANSACTION;
            }
        } else switch (item.getResultNo()) {
            case 1:
                option = Option.ISOLATION;
                break;
            case 2:
                option = Option.READ_ONLY;
                break;
            case 3:
                option = Option.DEFERRABLE;
                break;
            default:
                throw new IllegalArgumentException(String.format("unexpected  resultNo:%s", item.getResultNo()));
        }
        return option;
    }

    /**
     * @see #transactionStatus()
     */
    private Object transactionStatusValue(final ResultItem item) {
        final Object value;
        if (item instanceof ResultRowMeta) {
            value = ""; // here ,Pseudo value
        } else if (item instanceof ResultStates) {
            if (((ResultStates) item).hasMoreResult()) {
                value = ""; // here ,Pseudo value
            } else {
                value = ((ResultStates) item).inTransaction();
            }
        } else switch (item.getResultNo()) {
            case 1:
                value = parseIsolation(((ResultRow) item).getNonNull(0, String.class));
                break;
            case 2:
            case 3:
                value = parseBoolean(((ResultRow) item).getNonNull(0, String.class));
                break;
            default:
                throw new IllegalArgumentException(String.format("unexpected  resultNo:%s", item.getResultNo()));
        }
        return value;
    }

    /*-------------------below static method -------------------*/

    /**
     * @return true : isolation error
     * @see <a href="https://www.postgresql.org/docs/current/sql-start-transaction.html">START TRANSACTION</a>
     * @see <a href="https://www.postgresql.org/docs/current/sql-set-transaction.html">SET TRANSACTION</a>
     * @see <a href="https://www.postgresql.org/docs/current/sql-begin.html">BEGIN TRANSACTION</a>
     */
    private static boolean appendIsolation(final Isolation isolation, final StringBuilder builder) {

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
     * @see #transactionStatusValue(ResultItem)
     */
    private static Isolation parseIsolation(final String value) {
        final Isolation isolation;
        switch (value.toLowerCase(Locale.ROOT)) {
            case "read committed":
                isolation = Isolation.READ_COMMITTED;
                break;
            case "repeatable read":
                isolation = Isolation.REPEATABLE_READ;
                break;
            case "serializable":
                isolation = Isolation.SERIALIZABLE;
                break;
            case "read uncommitted":
                isolation = Isolation.READ_UNCOMMITTED;
                break;
            default:
                throw new IllegalArgumentException(String.format("unexpected isolation[%s]", value));
        }
        return isolation;
    }

    /**
     * @see #transactionStatusValue(ResultItem)
     */
    private static boolean parseBoolean(final String value) {
        final boolean on;
        switch (value.toLowerCase(Locale.ROOT)) {
            case "off":
            case "false":
                on = false;
                break;
            case "on":
            case "true":
                on = true;
                break;
            default:
                throw new IllegalArgumentException(String.format("unexpected boolean[%s]", value));
        }
        return on;
    }


}
