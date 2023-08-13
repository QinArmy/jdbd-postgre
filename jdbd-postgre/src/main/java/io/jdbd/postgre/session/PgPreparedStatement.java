package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.result.*;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.vendor.ResultType;
import io.jdbd.vendor.SubscribeException;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.task.PrepareTask;
import io.jdbd.vendor.util.JdbdBinds;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 * <p>
 * This class is a implementation of {@link PreparedStatement} with postgre client protocol.
 * </p>
 *
 * @since 1.0
 */
final class PgPreparedStatement extends PgParametrizedStatement<PreparedStatement> implements PreparedStatement {

    static PgPreparedStatement create(PgDatabaseSession<?> session, PrepareTask stmtTask) {
        return new PgPreparedStatement(session, stmtTask);
    }


    private final PrepareTask stmtTask;

    private final String sql;

    private final List<? extends DataType> paramTypeList;

    private final int paramCount;

    private final ResultRowMeta rowMeta;

    private final Warning warning;

    private List<List<ParamValue>> paramGroupList;

    private List<ParamValue> paramGroup;

    private PgPreparedStatement(PgDatabaseSession<?> session, PrepareTask stmtTask) {
        super(session);

        this.stmtTask = stmtTask;
        this.sql = stmtTask.getSql();
        this.paramTypeList = stmtTask.getParamTypes();
        this.paramCount = this.paramTypeList.size();

        this.rowMeta = stmtTask.getRowMeta();
        this.warning = stmtTask.getWarning();
    }


    @Override
    public List<? extends DataType> paramTypeList() {
        return this.paramTypeList;
    }

    @Override
    public ResultRowMeta resultRowMeta() {
        return this.rowMeta;
    }

    @Override
    public Warning waring() {
        return this.warning;
    }

    @Override
    public PreparedStatement bind(final int indexBasedZero, final @Nullable DataType dataType, final @Nullable Object value)
            throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw PgExceptions.cannotReuseStatement(PreparedStatement.class);
        }

        final int paramCount = this.paramCount;
        final RuntimeException error;
        final DataType type;

        if (indexBasedZero < 0 || indexBasedZero == paramCount) {
            List<List<ParamValue>> paramGroupList = this.paramGroupList;
            final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();
            error = PgExceptions.invalidParameterValue(groupSize, indexBasedZero);
        } else if (dataType == null) {
            error = PgExceptions.dataTypeIsNull();
        } else if (value != null && dataType == JdbdType.NULL) {
            error = PgExceptions.nonNullBindValueOf(dataType);
        } else if ((type = mapDataType(dataType)) == null) {
            error = PgExceptions.dontSupportDataType(dataType, PgDriver.POSTGRE_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = PgCollections.arrayList(paramCount);
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, value));
        }

        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            clearStatementToAvoidReuse();
            throw error;
        }
        return this;
    }

    @Override
    public PreparedStatement addBatch() throws JdbdException {
        final int paramCount = this.paramCount;
        final List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();


        // add bind group
        final JdbdException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramSize != paramCount) {
            error = PgExceptions.parameterCountMatch(groupSize, paramCount, paramSize);
        } else {
            if (paramGroupList == null) {
                this.paramGroupList = paramGroupList = PgCollections.arrayList();
            }
            if (paramGroup == null) {
                error = null;
                paramGroupList.add(EMPTY_PARAM_GROUP);
            } else {
                error = JdbdBinds.sortAndCheckParamGroup(groupSize, paramGroup);
                paramGroupList.add(PgCollections.unmodifiableList(paramGroup));
            }
        }

        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            clearStatementToAvoidReuse();
            throw PgExceptions.wrap(error);
        }

        this.paramGroup = null; // clear for next batch item
        return this;
    }

    @Override
    public Publisher<ResultStates> executeUpdate() {
        List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH_UPDATE);
        } else if (paramSize != this.paramCount) {
            error = PgExceptions.parameterCountMatch(0, this.paramCount, paramSize);
        } else if (paramGroup == null) {
            error = null;
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }


        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final Mono<ResultStates> mono;
        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            mono = Mono.error(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {
            this.fetchSize = 0;
            final ParamStmt stmt = Stmts.paramStmt(this.sql, paramGroup, this);
            this.stmtTask.suspendTask(); // must suspend task for query unknown type task
            mono = this.session.protocol.queryUnknownTypesIfNeed(unknownTypeSet)
                    .then(Mono.defer(() -> this.stmtTask.executeUpdate(stmt)));
        } else {
            this.fetchSize = 0;
            mono = this.stmtTask.executeUpdate(Stmts.paramStmt(this.sql, paramGroup, this));
        }
        clearStatementToAvoidReuse();
        return mono;
    }

    @Override
    public Publisher<ResultRow> executeQuery() {
        return this.executeQuery(DatabaseProtocol.ROW_FUNC, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(Function<CurrentRow, R> function) {
        return this.executeQuery(function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Publisher<R> executeQuery(final @Nullable Function<CurrentRow, R> function,
                                         final @Nullable Consumer<ResultStates> statesConsumer) {
        List<ParamValue> paramGroup = this.paramGroup;
        final int paramSize = paramGroup == null ? 0 : paramGroup.size();

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
        } else if (paramSize != this.paramCount) {
            error = PgExceptions.parameterCountMatch(0, this.paramCount, paramSize);
        } else if (function == null) {
            error = PgExceptions.queryMapFuncIsNull();
        } else if (statesConsumer == null) {
            error = PgExceptions.statesConsumerIsNull();
        } else if (paramGroup == null) {
            error = null;
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final Flux<R> flux;
        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamStmt stmt = Stmts.paramStmt(this.sql, paramGroup, statesConsumer, this);
            this.stmtTask.suspendTask(); // must suspend task for query unknown type task
            flux = this.session.protocol.queryUnknownTypesIfNeed(unknownTypeSet)
                    .thenMany(Flux.defer(() -> this.stmtTask.executeQuery(stmt, function)));
        } else {
            flux = this.stmtTask.executeQuery(Stmts.paramStmt(this.sql, paramGroup, this), function);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public Publisher<ResultStates> executeBatchUpdate() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = PgExceptions.noAnyParamGroupError();
        } else if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
        } else {
            error = null;
        }

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final Flux<ResultStates> flux;
        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {
            this.fetchSize = 0;
            final ParamBatchStmt stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            this.stmtTask.suspendTask(); // must suspend task for query unknown type task
            flux = this.session.protocol.queryUnknownTypesIfNeed(unknownTypeSet)
                    .thenMany(Flux.defer(() -> this.stmtTask.executeBatchUpdate(stmt)));
        } else {
            this.fetchSize = 0;
            flux = this.stmtTask.executeBatchUpdate(Stmts.paramBatch(this.sql, paramGroupList, this));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public BatchQuery executeBatchQuery() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = PgExceptions.noAnyParamGroupError();
        } else if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.BATCH_QUERY, ResultType.UPDATE);
        } else {
            error = null;
        }


        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final BatchQuery batchQuery;
        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            batchQuery = MultiResults.batchQueryError(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamBatchStmt stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            this.stmtTask.suspendTask(); // must suspend task for query unknown type task
            final Mono<Void> mono;
            mono = this.session.protocol.queryUnknownTypesIfNeed(unknownTypeSet);
            batchQuery = MultiResults.deferBatchQuery(mono, () -> this.stmtTask.executeBatchQuery(stmt));

        } else {
            batchQuery = this.stmtTask.executeBatchQuery(Stmts.paramBatch(this.sql, paramGroupList, this));
        }
        clearStatementToAvoidReuse();
        return batchQuery;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = PgExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        if (this.rowMeta == null) {
            this.fetchSize = 0;
        }

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final MultiResult multiResult;
        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            multiResult = MultiResults.multiError(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamBatchStmt stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            this.stmtTask.suspendTask(); // must suspend task for query unknown type task
            final Mono<Void> mono;
            mono = this.session.protocol.queryUnknownTypesIfNeed(unknownTypeSet);
            multiResult = MultiResults.deferMulti(mono, () -> this.stmtTask.executeBatchAsMulti(stmt));

        } else {
            multiResult = this.stmtTask.executeBatchAsMulti(Stmts.paramBatch(this.sql, paramGroupList, this));
        }
        clearStatementToAvoidReuse();
        return multiResult;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(PreparedStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null || paramGroupList.size() == 0) {
            error = PgExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        if (this.rowMeta == null) {
            this.fetchSize = 0;
        }

        final OrderedFlux flux;
        if (error != null) {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = MultiResults.fluxError(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamBatchStmt stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            this.stmtTask.suspendTask(); // must suspend task for query unknown type task
            final Mono<Void> mono;
            mono = this.session.protocol.queryUnknownTypesIfNeed(unknownTypeSet);
            flux = MultiResults.deferFlux(mono, () -> this.stmtTask.executeBatchAsFlux(stmt));

        } else {
            flux = this.stmtTask.executeBatchAsFlux(Stmts.paramBatch(this.sql, paramGroupList, this));
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public DatabaseSession abandonBind() {
        this.clearStatementToAvoidReuse();
        this.stmtTask.abandonBind();
        return this.session;
    }


    @Override
    void closeOnBindError(Throwable error) {
        this.stmtTask.closeOnBindError(error);
        this.clearStatementToAvoidReuse();
    }


    /*################################## blow private method ##################################*/

    private void clearStatementToAvoidReuse() {
        this.paramGroupList = null;
        this.unknownTypeSet = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
    }

}
