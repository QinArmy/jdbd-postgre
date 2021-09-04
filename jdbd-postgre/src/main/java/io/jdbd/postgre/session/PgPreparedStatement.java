package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.stmt.PrepareStmtTask;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.result.*;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdParamValue;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdBinds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.function.Consumer;


/**
 * <p>
 * This class is a implementation of {@link PreparedStatement} with postgre client protocol.
 * </p>
 */
final class PgPreparedStatement extends PgStatement implements PreparedStatement {

    static PgPreparedStatement create(PgDatabaseSession session, PrepareStmtTask stmtTask) {
        return new PgPreparedStatement(session, stmtTask);
    }


    private final PrepareStmtTask stmtTask;

    private final String sql;

    private final List<PgType> paramTypeList;

    private final int paramCount;

    private final ResultRowMeta rowMeta;

    private final List<List<ParamValue>> paramGroupList = new LinkedList<>();

    private List<ParamValue> paramGroup;

    private int fetchSize;

    private PgPreparedStatement(PgDatabaseSession session, PrepareStmtTask stmtTask) {
        super(session);
        this.stmtTask = stmtTask;
        this.sql = stmtTask.getSql();
        this.paramTypeList = stmtTask.getParamTypeList();
        this.paramCount = this.paramTypeList.size();

        this.rowMeta = stmtTask.getRowMeta();
        if (this.paramCount == 0) {
            this.paramGroup = Collections.emptyList();
        } else {
            this.paramGroup = new ArrayList<>(this.paramCount);
        }
    }


    @Override
    public final List<? extends SQLType> getParameterMeta() {
        return this.paramTypeList;
    }

    @Override
    public final ResultRowMeta getResultRowMeta() throws JdbdSQLException {
        final ResultRowMeta meta = this.rowMeta;
        if (meta == null) {
            throw PgExceptions.noReturnColumn();
        }
        return meta;
    }

    @Override
    public final void bind(final int indexBasedZero, final @Nullable Object nullable)
            throws JdbdException {
        this.paramGroup.add(JdbdParamValue.wrap(checkBindIndex(indexBasedZero), nullable));
    }


    @Override
    public final void addBatch() throws JdbdException {
        final List<ParamValue> paramGroup = this.paramGroup;
        final int paramCount = this.paramCount;

        final JdbdException error;
        if (paramGroup.size() != paramCount) {
            error = PgExceptions.parameterCountMatch(this.paramGroupList.size()
                    , this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(this.paramGroupList.size(), paramGroup);
        }
        if (error == null) {
            this.paramGroupList.add(paramGroup);
            if (paramCount > 0) {
                this.paramGroup = new ArrayList<>(paramCount);
            }
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }

    }


    @Override
    public final Mono<ResultStates> executeUpdate() {

        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = PgExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Mono<ResultStates> mono;
        if (error == null) {
            ParamStmt stmt = PgStmts.paramStmt(this.sql, paramGroup, this);
            mono = this.stmtTask.executeUpdate(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            mono = Mono.error(error);
        }
        return mono;
    }

    @Override
    public final Flux<ResultRow> executeQuery() {
        return executeQuery(PgFunctions.noActionConsumer());
    }

    @Override
    public final Flux<ResultRow> executeQuery(final Consumer<ResultStates> statesConsumer) {
        Objects.requireNonNull(statesConsumer, "statesConsumer");

        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = PgExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Flux<ResultRow> flux;
        if (error == null) {
            ParamStmt stmt = PgStmts.paramStmt(this.sql, paramGroup, statesConsumer, this);
            flux = this.stmtTask.executeQuery(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        }
        return flux;
    }

    @Override
    public final Flux<ResultStates> executeBatch() {
        final Flux<ResultStates> flux;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        } else {
            ParamBatchStmt<ParamValue> stmt;
            stmt = PgStmts.paramBatch(this.sql, this.paramGroupList, this);
            flux = this.stmtTask.executeBatch(stmt);
        }
        return flux;
    }

    @Override
    public final MultiResult executeBatchAsMulti() {
        final MultiResult result;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            result = MultiResults.error(error);
        } else {
            ParamBatchStmt<ParamValue> stmt;
            stmt = PgStmts.paramBatch(this.sql, this.paramGroupList, this);
            result = this.stmtTask.executeBatchAsMulti(stmt);
        }
        return result;
    }

    @Override
    public final Flux<Result> executeBatchAsFlux() {
        final Flux<Result> flux;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        } else {
            ParamBatchStmt<ParamValue> stmt;
            stmt = PgStmts.paramBatch(this.sql, this.paramGroupList, this);
            flux = this.stmtTask.executeBatchAsFlux(stmt);
        }
        return flux;
    }



    /*################################## blow Statement method ##################################*/

    @Override
    public final boolean supportPublisher() {
        return true;
    }


    @Override
    public final boolean setFetchSize(final int fetchSize) {
        final boolean support = this.rowMeta != null && fetchSize > 0;
        if (support) {
            this.fetchSize = fetchSize;
        } else {
            this.fetchSize = 0;
        }
        return support;
    }


    /*################################## blow StatementOption method ##################################*/

    @Override
    public final int getFetchSize() {
        return this.fetchSize;
    }

    /*################################## blow private method ##################################*/


    /**
     * <p>
     * If error ,this method invoke {@link PrepareStmtTask#closeOnBindError(Throwable)} before throw {@link Throwable}.
     * </p>
     *
     * @throws JdbdSQLException when indexBasedZero error
     */
    private int checkBindIndex(final int indexBasedZero) throws JdbdSQLException {
        if (indexBasedZero < 0 || indexBasedZero >= this.paramCount) {
            final JdbdException error = PgExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        return indexBasedZero;
    }


}
