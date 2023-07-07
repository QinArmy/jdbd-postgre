package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BindMultiStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.BatchQuery;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.MultiStatement;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * <p>
 * This interface is a implementation of {@link MultiStatement} with MySQL client protocol.
 * </p>
 *
 * @since 1.0
 */
final class MySQLMultiStatement extends MySQLStatement<MultiStatement> implements MultiStatement {

    static MySQLMultiStatement create(MySQLDatabaseSession<?> session) {
        return new MySQLMultiStatement(session);
    }

    private MySQLMultiStatement(MySQLDatabaseSession<?> session) {
        super(session);
    }

    private final List<ParamStmt> stmtGroup = MySQLCollections.arrayList();

    private String currentSql;

    /**
     * current bind group.
     */
    private List<ParamValue> paramGroup = null;


    @Override
    public MultiStatement addStatement(final String sql) throws JdbdException {

        this.endStmtOption();

        final List<ParamValue> paramGroup = this.paramGroup;
        final RuntimeException error;

        if (paramGroup == EMPTY_PARAM_GROUP) {
            error = MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        } else if (MySQLStrings.hasText(sql)) {
            error = null;
        } else {
            error = MySQLExceptions.sqlIsEmpty();
        }

        if (error != null) {
            clearStatementToAvoidReuse();
            throw MySQLExceptions.wrap(error);
        }

        final String lastSql = this.currentSql;
        if (lastSql != null) {
            this.stmtGroup.add(Stmts.paramStmt(lastSql, paramGroup, this));
        }

        this.currentSql = sql;
        this.paramGroup = null;
        return this;
    }


    @Override
    public MultiStatement bind(final int indexBasedZero, final @Nullable DataType dataType,
                               final @Nullable Object nullable) throws JdbdException {
        checkReuse();
        final List<BindValue> bindGroup = this.paramGroup;
        if (bindGroup == null) {
            throw MySQLExceptions.sqlIsEmpty();
        }
        if (!(dataType instanceof MySQLType)) {
            String m = String.format("sqlType isn't a instance of %s", MySQLType.class.getName());
            throw new MySQLJdbdException(m);
        }
        if (indexBasedZero < 0) {
            throw MySQLExceptions.invalidParameterValue(this.stmtGroup.size(), indexBasedZero);
        }
        bindGroup.add(BindValue.wrap(indexBasedZero, (MySQLType) dataType, nullable));
        return this;
    }


    @Override
    public Flux<ResultStates> executeBatchUpdate() {
        final List<BindStmt> stmtGroup = this.stmtGroup;
        final Flux<ResultStates> flux;
        if (this.paramGroup == null && stmtGroup.size() > 0) {
            flux = Flux.error(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (stmtGroup.size() == 0) {
            flux = Flux.error(MySQLExceptions.noAnyParamGroupError());
        } else {
            final BindMultiStmt stmt = Stmts.multi(stmtGroup, this.statementOption);
            flux = this.session.protocol.multiStmtBatch(stmt);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public BatchQuery executeBatchQuery() {
        return null;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final List<BindStmt> stmtGroup = this.stmtGroup;
        final MultiResult result;
        if (this.paramGroup == null && stmtGroup.size() > 0) {
            result = MultiResults.error(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (stmtGroup.size() == 0) {
            result = MultiResults.error(MySQLExceptions.noAnyParamGroupError());
        } else {
            final BindMultiStmt stmt = Stmts.multi(stmtGroup, this.statementOption);
            result = this.session.protocol.multiStmtAsMulti(stmt);
        }
        clearStatementToAvoidReuse();
        return result;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        final List<BindStmt> stmtGroup = this.stmtGroup;
        final OrderedFlux flux;
        if (this.paramGroup == null && stmtGroup.size() > 0) {
            flux = MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (stmtGroup.size() == 0) {
            flux = MultiResults.fluxError(MySQLExceptions.noAnyParamGroupError());
        } else {
            final BindMultiStmt stmt = Stmts.multi(stmtGroup, this.statementOption);
            flux = this.session.protocol.multiStmtAsFlux(stmt);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    /*################################## blow Statement method ##################################*/

    @Override
    public boolean supportPublisher() {
        // always false,MySQL COM_QUERY protocol don't support Publisher
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        // always false,MySQL COM_QUERY protocol don't support.
        return false;
    }

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ session : ")
                .append(this.session)
                .append(" , sqlList size : ")
                .append(this.stmtGroup.size())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }

    /*################################## blow MySQLStatement packet template method ##################################*/

    @Override
    void checkReuse() throws JdbdSQLException {
        if (this.paramGroup == null && this.stmtGroup.size() > 0) {
            throw MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        }
    }

    /*################################## blow private method ##################################*/

    private void clearStatementToAvoidReuse() {
        this.currentSql = null;
        this.paramGroup = EMPTY_PARAM_GROUP;
        this.stmtGroup.clear();
    }


}
