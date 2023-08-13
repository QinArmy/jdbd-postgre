package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.*;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.Parameter;
import io.jdbd.vendor.ResultType;
import io.jdbd.vendor.SubscribeException;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.*;
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
 * This class is a implementation of {@link BindStatement} with postgre client protocol.
 * </p>
 *
 * @see PgDatabaseSession#bindStatement(String)
 */
final class PgBindStatement extends PgParametrizedStatement<BindStatement> implements BindStatement {

    /**
     * @see PgDatabaseSession#bindStatement(String)
     */
    static PgBindStatement create(String sql, PgDatabaseSession<?> session, boolean forceServerPrepared) {
        if (!PgStrings.hasText(sql)) {
            throw new IllegalArgumentException("sql must be have text.");
        }
        return new PgBindStatement(sql, session, forceServerPrepared);
    }

    private final String sql;

    private final boolean forcePrepare;

    private List<List<ParamValue>> paramGroupList;

    private List<ParamValue> paramGroup;

    private int firstParamSize = -1;

    private boolean usePrepare;

    /**
     * private constructor
     */
    private PgBindStatement(String sql, PgDatabaseSession<?> session, boolean forcePrepare) {
        super(session);
        this.sql = sql;
        this.forcePrepare = forcePrepare;
    }

    @Override
    public boolean isForcePrepare() {
        return this.forcePrepare;
    }


    @Override
    public BindStatement bind(final int indexBasedZero, final @Nullable DataType dataType, final @Nullable Object value)
            throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw PgExceptions.cannotReuseStatement(BindStatement.class);
        }

        final int firstGroupSize = this.firstParamSize;

        final RuntimeException error;
        final DataType type;

        if (indexBasedZero < 0) {
            List<List<ParamValue>> paramGroupList = this.paramGroupList;
            final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();
            error = PgExceptions.invalidParameterValue(groupSize, indexBasedZero);
        } else if (indexBasedZero == firstGroupSize) {
            List<List<ParamValue>> paramGroupList = this.paramGroupList;
            final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();
            error = PgExceptions.notMatchWithFirstParamGroupCount(groupSize, indexBasedZero, firstGroupSize);
        } else if (dataType == null) {
            error = PgExceptions.dataTypeIsNull();
        } else if (dataType == JdbdType.NULL && value != null) {
            error = PgExceptions.nonNullBindValueOf(dataType);
        } else if ((type = mapDataType(dataType)) == null) {
            error = PgExceptions.dontSupportDataType(dataType, PgDriver.POSTGRE_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = PgCollections.arrayList(firstGroupSize < 0 ? 0 : firstGroupSize);
            }
            if (value instanceof Parameter) { // TODO long string or long binary or out parameter
                this.usePrepare = true;
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, value));
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw PgExceptions.wrap(error);
        }
        return this;
    }

    @Override
    public BindStatement addBatch() throws JdbdException {
        // add bind group
        final List<ParamValue> paramGroup = this.paramGroup;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            throw PgExceptions.cannotReuseStatement(BindStatement.class);
        }
        List<List<ParamValue>> paramGroupList = this.paramGroupList;

        final int paramSize = paramGroup == null ? 0 : paramGroup.size();
        final int groupSize = paramGroupList == null ? 0 : paramGroupList.size();

        int firstParamSize = this.firstParamSize;
        if (firstParamSize < 0) {
            this.firstParamSize = firstParamSize = paramSize;
        }

        final JdbdException error;
        if (paramSize != firstParamSize) {
            error = PgExceptions.notMatchWithFirstParamGroupCount(groupSize, paramSize, firstParamSize);
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(groupSize, paramGroup);
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw error;
        }

        if (paramGroupList == null) {
            this.paramGroupList = paramGroupList = PgCollections.arrayList();
        }

        if (paramGroup == null) {
            paramGroupList.add(EMPTY_PARAM_GROUP);
        } else {
            paramGroupList.add(PgCollections.unmodifiableList(paramGroup));
        }
        this.paramGroup = null; // clear for next batch
        return this;
    }


    @Override
    public Publisher<ResultStates> executeUpdate() {
        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH_UPDATE);
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        this.fetchSize = 0; //clear ,update don't need

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final Mono<ResultStates> mono;
        if (error != null) {
            mono = Mono.error(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final boolean severPrepare = isUsePrepare();
            final ParamStmt stmt = Stmts.paramStmt(this.sql, paramGroup, this);

            final PgProtocol protocol = this.session.protocol;

            mono = protocol.queryUnknownTypesIfNeed(unknownTypeSet)
                    .then(Mono.defer(() -> protocol.paramUpdate(stmt, severPrepare)));
        } else {
            mono = this.session.protocol.paramUpdate(Stmts.paramStmt(this.sql, paramGroup, this), isUsePrepare());
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
        final List<ParamValue> paramGroup = this.paramGroup;
        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.paramGroupList != null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
        } else if (function == null) {
            error = PgExceptions.queryMapFuncIsNull();
        } else if (statesConsumer == null) {
            error = PgExceptions.statesConsumerIsNull();
        } else if (paramGroup == null) {
            error = null;
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }

        final Set<String> unknownTypeSet = this.unknownTypeSet;

        final ParamStmt stmt;
        final Flux<R> flux;
        if (error != null) {
            flux = Flux.error(error); //don't wrap, maybe is NullPointerException
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final boolean severPrepare = isUsePrepare();
            stmt = Stmts.paramStmt(this.sql, paramGroup, statesConsumer, this);

            final PgProtocol protocol = this.session.protocol;

            flux = protocol.queryUnknownTypesIfNeed(unknownTypeSet)
                    .thenMany(Flux.defer(() -> protocol.paramQuery(stmt, severPrepare, function)));
        } else {
            stmt = Stmts.paramStmt(this.sql, paramGroup, this);
            flux = this.session.protocol.paramQuery(stmt, isUsePrepare(), function);
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    @Override
    public Publisher<ResultStates> executeBatchUpdate() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = PgExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        this.fetchSize = 0; //clear ,update don't need

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final ParamBatchStmt stmt;

        final Flux<ResultStates> flux;
        if (error != null) {
            flux = Flux.error(error);
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {
            final boolean severPrepare = isUsePrepare();
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);

            final PgProtocol protocol = this.session.protocol;

            flux = protocol.queryUnknownTypesIfNeed(unknownTypeSet)
                    .thenMany(Flux.defer(() -> protocol.paramBatchUpdate(stmt, severPrepare)));
        } else {

            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            flux = this.session.protocol.paramBatchUpdate(stmt, isUsePrepare());
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public BatchQuery executeBatchQuery() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = PgExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final ParamBatchStmt stmt;

        final BatchQuery batchQuery;
        if (error != null) {
            batchQuery = MultiResults.batchQueryError(PgExceptions.wrap(error));
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final boolean severPrepare = isUsePrepare();
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);

            final PgProtocol protocol = this.session.protocol;

            final Mono<Void> mono;
            mono = protocol.queryUnknownTypesIfNeed(unknownTypeSet);

            batchQuery = MultiResults.deferBatchQuery(mono, () -> protocol.paramBatchQuery(stmt, severPrepare));
        } else {
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            batchQuery = this.session.protocol.paramBatchQuery(stmt, isUsePrepare());
        }
        clearStatementToAvoidReuse();
        return batchQuery;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = PgExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final ParamBatchStmt stmt;

        final MultiResult multiResult;
        if (error != null) {
            multiResult = MultiResults.error(error);
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {
            final boolean severPrepare = isUsePrepare();
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);

            final PgProtocol protocol = this.session.protocol;

            final Mono<Void> mono;
            mono = protocol.queryUnknownTypesIfNeed(unknownTypeSet);

            multiResult = MultiResults.deferMulti(mono, () -> protocol.paramBatchAsMulti(stmt, severPrepare));
        } else {
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            multiResult = this.session.protocol.paramBatchAsMulti(stmt, isUsePrepare());
        }
        clearStatementToAvoidReuse();
        return multiResult;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        final List<List<ParamValue>> paramGroupList = this.paramGroupList;
        final List<ParamValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(BindStatement.class);
        } else if (paramGroup != null) {
            error = PgExceptions.noInvokeAddBatch();
        } else if (paramGroupList == null) {
            error = PgExceptions.noAnyParamGroupError();
        } else {
            error = null;
        }

        final Set<String> unknownTypeSet = this.unknownTypeSet;
        final ParamBatchStmt stmt;

        final OrderedFlux flux;
        if (error != null) {
            flux = MultiResults.fluxError(error);
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {
            final boolean severPrepare = isUsePrepare();
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);

            final PgProtocol protocol = this.session.protocol;

            final Mono<Void> mono;
            mono = protocol.queryUnknownTypesIfNeed(unknownTypeSet);

            flux = MultiResults.deferFlux(mono, () -> protocol.paramBatchAsFlux(stmt, severPrepare));
        } else {
            stmt = Stmts.paramBatch(this.sql, paramGroupList, this);
            flux = this.session.protocol.paramBatchAsFlux(stmt, isUsePrepare());
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    @Override
    public String toString() {
        return PgStrings.builder()
                .append(getClass().getName())
                .append("[ session : ")
                .append(this.session)
                .append(" , sql : ")
                .append(this.sql)
                .append(" , forcePrepare : ")
                .append(this.forcePrepare)
                .append(" , usePrepare : ")
                .append(this.usePrepare)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


    /*################################## blow private method ##################################*/


    private boolean isUsePrepare() {
        return this.forcePrepare || this.usePrepare || this.fetchSize > 0;
    }

    private void clearStatementToAvoidReuse() {
        this.paramGroupList = null;
        this.unknownTypeSet = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
    }

}
