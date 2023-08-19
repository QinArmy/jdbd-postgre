package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.JdbdType;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.BatchQuery;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.MultiStatement;
import io.jdbd.statement.Parameter;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

/**
 * <p>
 * This class is a implementation of {@link MultiStatement} with postgre client protocol.
 * </p>
 *
 * @see PgDatabaseSession#multiStatement()
 */
final class PgMultiStatement extends PgParametrizedStatement<MultiStatement> implements MultiStatement {

    /**
     * @see PgDatabaseSession#multiStatement()
     */
    static PgMultiStatement create(PgDatabaseSession<?> session) {
        return new PgMultiStatement(session);
    }

    /**
     * must initialize
     */
    private List<ParamStmt> stmtGroup = PgCollections.arrayList();

    private String currentSql;

    private List<ParamValue> paramGroup;

    private PgMultiStatement(PgDatabaseSession<?> session) {
        super(session);
    }

    @Override
    public MultiStatement addStatement(final String sql) {
        final List<ParamValue> paramGroup = this.paramGroup;

        final RuntimeException error;
        if (!PgStrings.hasText(sql)) {
            error = new IllegalArgumentException("Sql must have text.");
        } else if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(MultiStatement.class);
        } else if (paramGroup != null) {
            error = PgBinds.sortAndCheckParamGroup(this.stmtGroup.size(), paramGroup);
        } else {
            error = null;
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw error;
        }

        final String currentSql = this.currentSql;
        if (currentSql != null) {
            if (paramGroup == null) {
                this.stmtGroup.add(Stmts.paramStmt(currentSql, EMPTY_PARAM_GROUP, this));
            } else {
                this.stmtGroup.add(Stmts.paramStmt(currentSql, paramGroup, this));
            }
            this.resetChunkOptions();
        }
        this.currentSql = sql;
        this.paramGroup = null;
        return this;
    }


    @Override
    public MultiStatement bind(final int indexBasedZero, final @Nullable DataType dataType, final @Nullable Object value)
            throws JdbdException {

        List<ParamValue> paramGroup = this.paramGroup;

        final DataType type;

        final RuntimeException error;
        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = PgExceptions.cannotReuseStatement(MultiStatement.class);
        } else if (indexBasedZero < 0) {
            error = PgExceptions.invalidParameterValue(this.stmtGroup.size(), indexBasedZero);
        } else if (dataType == null) {
            error = PgExceptions.dataTypeIsNull();
        } else if (value instanceof Parameter) {
            error = PgExceptions.dontSupportOutParameter(indexBasedZero, MultiStatement.class, PgDriver.POSTGRE_SQL);
        } else if (dataType == JdbdType.NULL && value != null) {
            error = PgExceptions.nonNullBindValueOf(dataType);
        } else if ((type = mapDataType(dataType)) == null) {
            error = PgExceptions.dontSupportDataType(dataType, PgDriver.POSTGRE_SQL);
        } else {
            error = null;
            if (paramGroup == null) {
                this.paramGroup = paramGroup = PgCollections.arrayList();
            }
            paramGroup.add(JdbdValues.paramValue(indexBasedZero, type, value));
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw error;
        }
        return this;
    }


    @Override
    public Flux<ResultStates> executeBatchUpdate() {

        final List<ParamStmt> stmtGroup = this.stmtGroup;

        final Set<String> unknownTypeSet = this.unknownTypeSet;

        final Flux<ResultStates> flux;
        final JdbdException error;
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            flux = Flux.error(PgExceptions.cannotReuseStatement(MultiStatement.class));
        } else if ((error = endMultiStatement()) != null) { // end statement
            flux = Flux.error(error);
        } else if (stmtGroup.size() == 0) {
            flux = Flux.error(PgExceptions.multiStmtNoSql());
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamMultiStmt stmt = Stmts.paramMultiStmt(stmtGroup, this);
            final PgProtocol protocol = this.session.protocol;

            flux = protocol.queryUnknownTypesIfNeed(unknownTypeSet)
                    .thenMany(Flux.defer(() -> protocol.multiStmtBatchUpdate(stmt)));
        } else {
            flux = this.session.protocol.multiStmtBatchUpdate(Stmts.paramMultiStmt(stmtGroup, this));
        }
        this.clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public BatchQuery executeBatchQuery() {
        final List<ParamStmt> stmtGroup = this.stmtGroup;

        final Set<String> unknownTypeSet = this.unknownTypeSet;

        final BatchQuery batchQuery;
        final JdbdException error;
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            batchQuery = MultiResults.batchQueryError(PgExceptions.cannotReuseStatement(MultiStatement.class));
        } else if ((error = endMultiStatement()) != null) { // end statement
            batchQuery = MultiResults.batchQueryError(error);
        } else if (stmtGroup.size() == 0) {
            batchQuery = MultiResults.batchQueryError(PgExceptions.multiStmtNoSql());
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamMultiStmt stmt = Stmts.paramMultiStmt(stmtGroup, this);
            final PgProtocol protocol = this.session.protocol;

            final Mono<Void> mono;
            mono = protocol.queryUnknownTypesIfNeed(unknownTypeSet);

            batchQuery = MultiResults.deferBatchQuery(mono, () -> protocol.multiStmtBatchQuery(stmt));
        } else {
            batchQuery = this.session.protocol.multiStmtBatchQuery(Stmts.paramMultiStmt(stmtGroup, this));
        }
        this.clearStatementToAvoidReuse();
        return batchQuery;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final List<ParamStmt> stmtGroup = this.stmtGroup;

        final Set<String> unknownTypeSet = this.unknownTypeSet;

        final MultiResult multiResult;
        final JdbdException error;
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            multiResult = MultiResults.multiError(PgExceptions.cannotReuseStatement(MultiStatement.class));
        } else if ((error = endMultiStatement()) != null) { // end statement
            multiResult = MultiResults.multiError(error);
        } else if (stmtGroup.size() == 0) {
            multiResult = MultiResults.multiError(PgExceptions.multiStmtNoSql());
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamMultiStmt stmt = Stmts.paramMultiStmt(stmtGroup, this);
            final PgProtocol protocol = this.session.protocol;

            final Mono<Void> mono;
            mono = protocol.queryUnknownTypesIfNeed(unknownTypeSet);

            multiResult = MultiResults.deferMulti(mono, () -> protocol.multiStmtAsMulti(stmt));
        } else {
            multiResult = this.session.protocol.multiStmtAsMulti(Stmts.paramMultiStmt(stmtGroup, this));
        }
        this.clearStatementToAvoidReuse();
        return multiResult;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        final List<ParamStmt> stmtGroup = this.stmtGroup;

        final Set<String> unknownTypeSet = this.unknownTypeSet;

        final OrderedFlux flux;
        final JdbdException error;
        if (this.paramGroup == EMPTY_PARAM_GROUP) {
            flux = MultiResults.fluxError(PgExceptions.cannotReuseStatement(MultiStatement.class));
        } else if ((error = endMultiStatement()) != null) { // end statement
            flux = MultiResults.fluxError(error);
        } else if (stmtGroup.size() == 0) {
            flux = MultiResults.fluxError(PgExceptions.multiStmtNoSql());
        } else if (unknownTypeSet != null
                && unknownTypeSet.size() > 0
                && this.session.protocol.isNeedQueryUnknownType(unknownTypeSet)) {

            final ParamMultiStmt stmt = Stmts.paramMultiStmt(stmtGroup, this);
            final PgProtocol protocol = this.session.protocol;

            final Mono<Void> mono;
            mono = protocol.queryUnknownTypesIfNeed(unknownTypeSet);

            flux = MultiResults.deferFlux(mono, () -> protocol.multiStmtAsFlux(stmt));
        } else {
            flux = this.session.protocol.multiStmtAsFlux(Stmts.paramMultiStmt(stmtGroup, this));
        }
        this.clearStatementToAvoidReuse();
        return flux;
    }


    /*################################## blow private method ##################################*/


    private void clearStatementToAvoidReuse() {
        this.stmtGroup = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
    }

    @Nullable
    private JdbdException endMultiStatement() {

        final String lastSql = this.currentSql;
        if (lastSql == null) {
            return PgExceptions.noInvokeAndAddStatement();
        }
        final List<ParamValue> paramGroup = this.paramGroup;
        if (paramGroup == null) {
            this.stmtGroup.add(Stmts.paramStmt(lastSql, EMPTY_PARAM_GROUP, this));
        } else {
            this.stmtGroup.add(Stmts.paramStmt(lastSql, paramGroup, this));
        }
        this.resetChunkOptions();
        this.currentSql = null;
        this.paramGroup = null;
        return null;
    }

}
