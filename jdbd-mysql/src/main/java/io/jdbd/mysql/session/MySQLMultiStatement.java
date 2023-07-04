package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.DataType;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.*;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.vendor.result.MultiResults;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.stmt.MultiStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLMultiStatement extends MySQLStatement implements AttrMultiStatement {

    static MySQLMultiStatement create(MySQLDatabaseSession session) {
        return new MySQLMultiStatement(session);
    }

    private MySQLMultiStatement(MySQLDatabaseSession session) {
        super(session);
    }

    private final List<BindStmt> stmtGroup = new ArrayList<>();

    /**
     * current bind group.
     */
    private List<BindValue> bindGroup = null;


    @Override
    public MultiStatement addStatement(final String sql) throws JdbdException {
        checkReuse();
        if (!MySQLStrings.hasText(sql)) {
            throw new IllegalArgumentException("Sql must have text.");
        }
        final List<BindValue> bindGroup = new ArrayList<>();
        this.stmtGroup.add(Stmts.elementOfMulti(sql, bindGroup));
        this.bindGroup = bindGroup;
        return this;
    }


    @Override
    public MultiStatement bind(final int indexBasedZero, final JDBCType jdbcType, @Nullable final Object nullable)
            throws JdbdException {
        checkReuse();
        final List<BindValue> bindGroup = this.bindGroup;
        if (bindGroup == null) {
            throw MySQLExceptions.createEmptySqlException();
        }
        if (indexBasedZero < 0) {
            throw MySQLExceptions.invalidParameterValue(this.stmtGroup.size(), indexBasedZero);
        }
        final MySQLType type = MySQLBinds.mapJdbcTypeToMySQLType(jdbcType, nullable);
        bindGroup.add(BindValue.wrap(indexBasedZero, type, nullable));
        return this;
    }

    @Override
    public MultiStatement bind(final int indexBasedZero, final DataType dataType, @Nullable final Object nullable)
            throws JdbdException {
        checkReuse();
        final List<BindValue> bindGroup = this.bindGroup;
        if (bindGroup == null) {
            throw MySQLExceptions.createEmptySqlException();
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
    public MultiStatement bind(final int indexBasedZero, @Nullable final Object nullable) throws JdbdException {
        checkReuse();
        final List<BindValue> bindGroup = this.bindGroup;
        if (bindGroup == null) {
            throw MySQLExceptions.createEmptySqlException();
        }
        if (indexBasedZero < 0) {
            throw MySQLExceptions.invalidParameterValue(this.stmtGroup.size(), indexBasedZero);
        }
        bindGroup.add(BindValue.wrap(indexBasedZero, MySQLBinds.inferMySQLType(nullable), nullable));
        return this;
    }

    @Override
    public Flux<ResultStates> executeBatchUpdate() {
        final List<BindStmt> stmtGroup = this.stmtGroup;
        final Flux<ResultStates> flux;
        if (this.bindGroup == null && stmtGroup.size() > 0) {
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
    public MultiResult executeBatchAsMulti() {
        final List<BindStmt> stmtGroup = this.stmtGroup;
        final MultiResult result;
        if (this.bindGroup == null && stmtGroup.size() > 0) {
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
        if (this.bindGroup == null && stmtGroup.size() > 0) {
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
    public boolean setFetchSize(int fetchSize) {
        // always false,MySQL ComQuery protocol don't support.
        return false;
    }

    /*################################## blow MySQLStatement packet template method ##################################*/

    @Override
    void checkReuse() throws JdbdSQLException {
        if (this.bindGroup == null && this.stmtGroup.size() > 0) {
            throw MySQLExceptions.cannotReuseStatement(MultiStatement.class);
        }
    }

    /*################################## blow private method ##################################*/

    private void clearStatementToAvoidReuse() {
        this.bindGroup = null;
    }


}
