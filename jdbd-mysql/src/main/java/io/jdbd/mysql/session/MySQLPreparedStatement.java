package io.jdbd.mysql.session;

import io.jdbd.DatabaseSession;
import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrPreparedStatement;
import io.jdbd.mysql.stmt.QueryAttr;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.JdbdParamValue;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.task.PrepareTask;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdFunctions;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.function.Consumer;

/**
 * <p>
 * This interface is a implementation of {@link PreparedStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLPreparedStatement extends MySQLStatement implements AttrPreparedStatement {

    static MySQLPreparedStatement create(MySQLDatabaseSession session, PrepareTask<MySQLType> task) {
        return new MySQLPreparedStatement(session, task);
    }

    private final String sql;

    private final PrepareTask<MySQLType> stmtTask;

    private final List<MySQLType> paramTypes;

    private final ResultRowMeta rowMeta;

    private final Warning warning;

    private final int paramCount;

    private final List<List<ParamValue>> paramGroupList = new ArrayList<>();

    private List<ParamValue> paramGroup;

    private Map<String, QueryAttr> attrGroup;

    private MySQLPreparedStatement(final MySQLDatabaseSession session, final PrepareTask<MySQLType> stmtTask) {
        super(session);
        this.sql = stmtTask.getSql();
        this.stmtTask = stmtTask;
        this.paramTypes = stmtTask.getParamTypes();
        this.rowMeta = stmtTask.getRowMeta();

        this.warning = stmtTask.getWarning();
        this.paramCount = this.paramTypes.size();

        if (this.paramCount == 0) {
            this.paramGroup = Collections.emptyList();
        } else {
            this.paramGroup = new ArrayList<>(this.paramCount);
        }

    }


    @Override
    public void bind(final int indexBasedZero, final @Nullable Object nullable) throws JdbdException {
        final List<ParamValue> paramGroup = this.paramGroup;
        if (paramGroup == null) {
            throw MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        }

        if (indexBasedZero < 0 || indexBasedZero >= this.paramCount) {
            final JdbdException error;
            error = MySQLExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        paramGroup.add(JdbdParamValue.wrap(indexBasedZero, nullable));
    }

    @Override
    public void bindCommonAttr(final String name, final MySQLType type, final @Nullable Object value) {
        if (this.paramGroup == null) {
            throw MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        }
        Objects.requireNonNull(name, "name");

        Map<String, QueryAttr> commonAttrGroup = this.statementOption.commonAttrGroup;
        if (commonAttrGroup == null) {
            commonAttrGroup = new HashMap<>();
            this.statementOption.commonAttrGroup = commonAttrGroup;
        }
        commonAttrGroup.put(name, QueryAttr.wrap(type, value));
    }

    @Override
    public void bindAttr(final String name, final MySQLType type, final @Nullable Object value) {
        if (this.paramGroup == null) {
            throw MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        }
        Objects.requireNonNull(name, "name");
        Map<String, QueryAttr> attrGroup = this.attrGroup;
        if (attrGroup == null) {
            attrGroup = new HashMap<>();
            this.attrGroup = attrGroup;
            prepareAttrGroupList(this.paramGroupList.size());
        }
        attrGroup.put(name, QueryAttr.wrap(type, value));
    }


    @Override
    public void addBatch() {
        final List<ParamValue> paramGroup = this.paramGroup;
        final int paramCount = this.paramCount;

        // add bind group
        final JdbdException error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup.size() != paramCount) {
            error = MySQLExceptions.parameterCountMatch(this.paramGroupList.size()
                    , this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(this.paramGroupList.size(), paramGroup);
        }

        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        this.paramGroupList.add(paramGroup);
        if (paramCount > 0) {
            this.paramGroup = new ArrayList<>(paramCount);
        }

        // add query attributes group
        List<Map<String, QueryAttr>> attrGroupList = this.statementOption.attrGroupList;

        final Map<String, QueryAttr> attrGroup = this.attrGroup;
        if (attrGroup == null) {
            if (attrGroupList != null) {
                attrGroupList.add(Collections.emptyMap());
            }
        } else {
            if (attrGroupList == null) {
                attrGroupList = new ArrayList<>();
                this.statementOption.attrGroupList = attrGroupList;
            }
            attrGroupList.add(attrGroup);
            this.attrGroup = null;
        }

    }

    @Override
    public List<? extends SQLType> getParameterTypes() {
        return this.paramTypes;
    }

    @Nullable
    @Override
    public ResultRowMeta getResultRowMeta() throws JdbdSQLException {
        return this.rowMeta;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        final List<ParamValue> paramGroup = this.paramGroup;

        final Throwable error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (this.paramGroupList.size() > 0 || !MySQLCollections.isEmpty(this.statementOption.attrGroupList)) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = MySQLExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
            final Map<String, QueryAttr> attrGroup = this.attrGroup;
            if (error == null && attrGroup != null) {
                prepareAttrGroup(attrGroup);
            }
        }

        final Mono<ResultStates> mono;
        if (error == null) {
            ParamStmt stmt = Stmts.paramStmt(this.sql, paramGroup, this.statementOption);
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
        return executeQuery(JdbdFunctions.noActionConsumer());
    }

    @Override
    public Flux<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        Objects.requireNonNull(statesConsumer, "statesConsumer");

        final List<ParamValue> paramGroup = this.paramGroup;

        final Throwable error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (this.paramGroupList.size() > 0 || !MySQLCollections.isEmpty(this.statementOption.attrGroupList)) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = MySQLExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
            final Map<String, QueryAttr> attrGroup = this.attrGroup;
            if (error == null && attrGroup != null) {
                prepareAttrGroup(attrGroup);
            }
        }

        final Flux<ResultRow> flux;
        if (error == null) {
            ParamStmt stmt = Stmts.paramStmt(this.sql, paramGroup, statesConsumer, this.statementOption);
            flux = this.stmtTask.executeQuery(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public Flux<ResultStates> executeBatch() {
        final Flux<ResultStates> flux;
        final int batchCount = this.paramGroupList.size();
        if (this.paramGroup == null) {
            flux = Flux.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (batchCount > 0) {
            final JdbdException error = MySQLExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        } else {
            final Throwable e;
            e = checkBatchAttrGroupListSize(batchCount);
            if (e == null) {
                final ParamBatchStmt<ParamValue> stmt;
                stmt = Stmts.paramBatch(this.sql, this.paramGroupList, this.statementOption);
                flux = this.stmtTask.executeBatch(stmt);
            } else {
                this.stmtTask.closeOnBindError(e); // close prepare statement.
                flux = Flux.error(e);
            }
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final MultiResult result;
        final int batchCount = this.paramGroupList.size();
        if (this.paramGroup == null) {
            result = MultiResults.error(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (batchCount > 0) {
            final JdbdException error = MySQLExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            result = MultiResults.error(error);
        } else {
            final Throwable e;
            if (this.statementOption.fetchSize > 0) {
                e = MySQLExceptions.batchAsMultiNonSupportFetch();
            } else {
                e = checkBatchAttrGroupListSize(batchCount);
            }
            if (e == null) {
                final ParamBatchStmt<ParamValue> stmt;
                stmt = Stmts.paramBatch(this.sql, this.paramGroupList, this.statementOption);
                result = this.stmtTask.executeBatchAsMulti(stmt);
            } else {
                this.stmtTask.closeOnBindError(e); // close prepare statement.
                result = MultiResults.error(e);
            }
        }
        clearStatementToAvoidReuse();
        return result;
    }


    @Override
    public OrderedFlux executeBatchAsFlux() {
        final OrderedFlux flux;
        final int batchCount = this.paramGroupList.size();
        if (this.paramGroup == null) {
            flux = MultiResults.orderedFluxError(MySQLExceptions.cannotReuseStatement(PreparedStatement.class));
        } else if (batchCount > 0) {
            final JdbdException error = MySQLExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = MultiResults.orderedFluxError(error);
        } else {
            final Throwable e;
            e = checkBatchAttrGroupListSize(batchCount);
            if (e == null) {
                final ParamBatchStmt<ParamValue> stmt;
                stmt = Stmts.paramBatch(this.sql, this.paramGroupList, this.statementOption);
                flux = this.stmtTask.executeBatchAsFlux(stmt);
            } else {
                this.stmtTask.closeOnBindError(e); // close prepare statement.
                flux = MultiResults.orderedFluxError(e);
            }
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    @Override
    public boolean setFetchSize(final int fetchSize) {
        this.statementOption.fetchSize = fetchSize;
        return fetchSize > 0;
    }

    @Override
    public boolean supportPublisher() {
        // always true.
        return true;
    }

    @Override
    public boolean supportOutParameter() {
        // always true.
        return true;
    }


    @Override
    public Publisher<DatabaseSession> abandonBind() {
        return this.stmtTask.abandonBind()
                .thenReturn(this.session);
    }


    @Override
    public Warning getWaring() {
        return this.warning;
    }


    /*################################## blow private method ##################################*/


    private void clearStatementToAvoidReuse() {
        this.paramGroup = null;
        this.attrGroup = null;

    }


}
