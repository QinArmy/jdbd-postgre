package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.ResultType;
import io.jdbd.statement.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdParamValue;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.task.PrepareTask;
import io.jdbd.vendor.util.JdbdBinds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.*;
import java.util.function.Consumer;


/**
 * <p>
 * This class is a implementation of {@link PreparedStatement} with postgre client protocol.
 * </p>
 */
final class PgPreparedStatement extends PgStatement implements PreparedStatement {

    static PgPreparedStatement create(PgDatabaseSession session, PrepareTask<PgType> stmtTask) {
        return new PgPreparedStatement(session, stmtTask);
    }


    private final PrepareTask<PgType> stmtTask;

    private final String sql;

    private final List<PgType> paramTypeList;

    private final int paramCount;

    private final ResultRowMeta rowMeta;

    private final Warning warning;

    private final List<List<ParamValue>> paramGroupList = new LinkedList<>();

    private List<ParamValue> paramGroup;

    private int fetchSize;

    private PgPreparedStatement(PgDatabaseSession session, PrepareTask<PgType> stmtTask) {
        super(session);
        this.stmtTask = stmtTask;
        this.sql = stmtTask.getSql();
        this.paramTypeList = stmtTask.getParamTypes();
        this.paramCount = this.paramTypeList.size();

        this.warning = stmtTask.getWarning();
        this.rowMeta = stmtTask.getRowMeta();
        if (this.paramCount == 0) {
            this.paramGroup = Collections.emptyList();
        } else {
            this.paramGroup = new ArrayList<>(this.paramCount);
        }
    }


    @Override
    public List<? extends DataType> paramTypeList() {
        return null;
    }

    @Override
    public PreparedStatement bind(final int indexBasedZero, final @Nullable Object nullable)
            throws JdbdException {
        final List<ParamValue> paramGroup = this.paramGroup;
        if (paramGroup == null) {
            throw PgExceptions.cannotReuseStatement(PreparedStatement.class);
        }
        if (indexBasedZero < 0 || indexBasedZero >= this.paramCount) {
            final JdbdException error = PgExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }

        paramGroup.add(JdbdParamValue.wrap(indexBasedZero, nullable));
        return this;
    }

    @Override
    public PreparedStatement bind(int indexBasedZero, JDBCType jdbcType,  final @Nullable Object nullable)
            throws JdbdException {
        return this;
    }

    @Override
    public PreparedStatement bind(int indexBasedZero, DataType dataType, final @Nullable Object value)
            throws JdbdException {
        return this;
    }

    @Override
    public PreparedStatement bind(int indexBasedZero, String dataTypeName,final @Nullable  Object nullable)
            throws JdbdException {
        return this;
    }

    @Override
    public PreparedStatement addBatch() throws JdbdException {
        final List<ParamValue> paramGroup = this.paramGroup;
        final int paramCount = this.paramCount;

        final JdbdException error;
        if (paramGroup == null) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup.size() != paramCount) {
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
        return this;
    }


    @Override
    public Mono<ResultStates> executeUpdate() {

        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (paramGroup == null) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH_UPDATE);
        } else if (paramGroup.size() != this.paramCount) {
            error = PgExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Mono<ResultStates> mono;
        if (error == null) {
            this.fetchSize = 0;
            ParamStmt stmt = PgStmts.paramStmt(this.sql, paramGroup, this);
            mono = this.stmtTask.executeUpdate(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            mono = Mono.error(error);
        }
        clearStatementToAvoidReuse();
        return mono;
    }


    @Override
    public Flux<ResultRow> executeQuery() {
        return executeQuery(PgFunctions.noActionConsumer());
    }

    @Override
    public Flux<ResultRow> executeQuery(final Consumer<ResultStates> statesConsumer) {
        Objects.requireNonNull(statesConsumer, "statesConsumer");

        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (paramGroup == null) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
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
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public Flux<ResultStates> executeBatchUpdate() {
        final Flux<ResultStates> flux;
        if (this.paramGroup == null) {
            flux = Flux.error(PgExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        } else {
            ParamBatchStmt<ParamValue> stmt;
            stmt = PgStmts.paramBatch(this.sql, this.paramGroupList, this);
            flux = this.stmtTask.executeBatchUpdate(stmt);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final MultiResult result;
        if (this.paramGroup == null) {
            result = MultiResults.error(PgExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            result = MultiResults.error(error);
        } else {
            ParamBatchStmt<ParamValue> stmt;
            stmt = PgStmts.paramBatch(this.sql, this.paramGroupList, this);
            result = this.stmtTask.executeBatchAsMulti(stmt);
        }
        clearStatementToAvoidReuse();
        return result;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        final OrderedFlux flux;
        if (this.paramGroup == null) {
            flux = MultiResults.fluxError(PgExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = MultiResults.fluxError(error);
        } else {
            ParamBatchStmt<ParamValue> stmt;
            stmt = PgStmts.paramBatch(this.sql, this.paramGroupList, this);
            flux = this.stmtTask.executeBatchAsFlux(stmt);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public Mono<DatabaseSession> abandonBind() {
        return this.stmtTask.abandonBind()
                .thenReturn(this.session);
    }

    @Nullable
    @Override
    public Warning waring() {
        return this.warning;
    }

    /*################################## blow Statement method ##################################*/

    @Override
    public boolean supportPublisher() {
        return true;
    }


    @Override
    public boolean setFetchSize(final int fetchSize) {
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
    public int getFetchSize() {
        return this.fetchSize;
    }

    /*################################## blow private method ##################################*/

    private void clearStatementToAvoidReuse() {
        this.paramGroup = null;
    }

}
