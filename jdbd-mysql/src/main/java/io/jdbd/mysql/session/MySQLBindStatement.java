package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.*;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.*;
import io.jdbd.statement.BindStatement;
import io.jdbd.statement.ResultType;
import io.jdbd.statement.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.util.JdbdBinds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.statement.BindStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLBindStatement extends MySQLStatement implements AttrBindStatement {

    static MySQLBindStatement create(final MySQLDatabaseSession session, final String sql) {
        return new MySQLBindStatement(session, sql);
    }

    private final String sql;

    private List<List<BindValue>> bindGroupList;

    private List<BindValue> bindGroup = MySQLCollections.arrayList();

    private int firstGroupSize = -1;

    public MySQLBindStatement(final MySQLDatabaseSession session, final String sql) {
        super(session);
        this.sql = sql;
    }

    @Override
    public BindStatement bind(final int indexBasedZero, @Nullable final Object nullable) throws JdbdException {
        checkReuse();
        this.bindGroup.add(BindValue.wrap(checkIndex(indexBasedZero), MySQLBinds.inferMySQLType(nullable), nullable));
        return this;
    }


    @Override
    public BindStatement bind(final int indexBasedZero, final DataType sqlType, final @Nullable Object nullable)
            throws JdbdException {
        checkReuse();
        if (!(sqlType instanceof MySQLType)) {
            String m = String.format("sqlType isn't a instance of %s", MySQLType.class.getName());
            throw new MySQLJdbdException(m);
        }
        this.bindGroup.add(BindValue.wrap(checkIndex(indexBasedZero), (MySQLType) sqlType, nullable));
        return this;
    }

    @Override
    public BindStatement bindStmtVar(final String name, final @Nullable Object nullable) throws JdbdException {
        return this;
    }

    @Override
    public BindStatement bindStmtVar(final String name, final DataType dataType, final @Nullable Object nullable)
            throws JdbdException {
        return this;
    }

    @Override
    public BindStatement addBatch() throws JdbdException {
        checkReuse();
        // add bind group
        final List<BindValue> paramGroup = this.bindGroup;
        int firstGroupSize = this.firstGroupSize;

        if (firstGroupSize < 0) {
            firstGroupSize = paramGroup.size();
            this.firstGroupSize = firstGroupSize;
        }

        if (paramGroup.size() != firstGroupSize) {
            throw MySQLExceptions.notMatchWithFirstParamGroupCount(this.bindGroupList.size()
                    , paramGroup.size(), firstGroupSize);
        } else {
            final JdbdException error = JdbdBinds.sortAndCheckParamGroup(this.bindGroupList.size(), paramGroup);
            if (error != null) {
                throw error;
            }
        }

        switch (paramGroup.size()) {
            case 0: {
                this.bindGroupList.add(Collections.emptyList());
            }
            break;
            case 1: {
                this.bindGroupList.add(Collections.singletonList(paramGroup.get(0)));
                this.bindGroup = MySQLCollections.arrayList(1);
            }
            break;
            default: {
                this.bindGroupList.add(Collections.unmodifiableList(paramGroup));
                this.bindGroup = MySQLCollections.arrayList(firstGroupSize);
            }
        }
        return this;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        final List<BindValue> paramGroup = this.bindGroup;
        final JdbdException error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.bindGroupList.size() > 0) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH);
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }
        final Mono<ResultStates> mono;
        if (error == null) {
            this.statementOption.fetchSize = 0;
            final BindStmt stmt;
            stmt = Stmts.bind(this.sql, paramGroup, this.statementOption);
            mono = this.session.protocol.bindUpdate(stmt, false);
        } else {
            mono = Mono.error(error);
        }
        clearStatementToAvoidReuse();
        return mono;
    }

    @Override
    public Flux<ResultRow> executeQuery() {
        return this.executeQuery(CurrentRow::asResultRow, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Flux<R> executeQuery(Function<CurrentRow, R> function) {
        return this.executeQuery(function, Stmts.IGNORE_RESULT_STATES);
    }

    @Override
    public <R> Flux<R> executeQuery(final Function<CurrentRow, R> function, final Consumer<ResultStates> consumer) {
        final List<BindValue> paramGroup = this.bindGroup;
        final JdbdException error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.bindGroupList != null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH);
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
        }
        final Flux<R> flux;
        if (error == null) {
            final BindStmt stmt;
            stmt = Stmts.bind(this.sql, paramGroup, this.statementOption);
            flux = this.session.protocol.bindQuery(stmt, false, function, consumer);
        } else {
            flux = Flux.error(error);
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    @Override
    public Flux<ResultStates> executeBatchUpdate() {
        final Flux<ResultStates> flux;
        if (this.bindGroup == null) {
            flux = Flux.error(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (this.bindGroupList.size() == 0) {
            flux = Flux.error(MySQLExceptions.noAnyParamGroupError());
        } else {
            this.statementOption.fetchSize = 0; // executeBatch() don't support fetch.
            final BindBatchStmt stmt;
            stmt = Stmts.bindBatch(this.sql, this.bindGroupList, this.statementOption);
            flux = this.session.protocol.bindBatch(stmt, false);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final MultiResult result;
        if (this.bindGroup == null) {
            result = MultiResults.error(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (this.bindGroupList.size() == 0) {
            result = MultiResults.error(MySQLExceptions.noAnyParamGroupError());
        } else {
            this.statementOption.fetchSize = 0; // executeBatchAsMulti() don't support fetch.
            final BindBatchStmt stmt;
            stmt = Stmts.bindBatch(this.sql, this.bindGroupList, this.statementOption);
            result = this.session.protocol.bindBatchAsMulti(stmt, false);
        }
        clearStatementToAvoidReuse();
        return result;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        final OrderedFlux flux;
        if (this.bindGroup == null) {
            flux = MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (this.bindGroupList.size() == 0) {
            flux = MultiResults.fluxError(MySQLExceptions.noAnyParamGroupError());
        } else {
            final BindBatchStmt stmt;
            stmt = Stmts.bindBatch(this.sql, this.bindGroupList, this.statementOption);
            flux = this.session.protocol.bindBatchAsFlux(stmt, false);
        }
        clearStatementToAvoidReuse();
        return flux;
    }


    /*################################## blow Statement method ##################################*/

    @Override
    public boolean supportPublisher() {
        // adapt to PreparedStatement
        return true;
    }

    @Override
    public boolean supportOutParameter() {
        // use PreparedStatement,because couldn't realize out parameter.
        return false;
    }

    @Override
    public boolean setFetchSize(final int fetchSize) {
        this.statementOption.fetchSize = fetchSize;
        return fetchSize > 0;
    }


    /*################################## blow packet template method ##################################*/

    @Override
    void checkReuse() throws JdbdSQLException {
        if (this.bindGroup == null) {
            throw MySQLExceptions.cannotReuseStatement(BindStatement.class);
        }
    }

    /*################################## blow private method ##################################*/

    /**
     * @throws JdbdSQLException when indexBasedZero error
     */
    private int checkIndex(final int indexBasedZero) throws JdbdSQLException {
        if (indexBasedZero < 0) {
            throw MySQLExceptions.invalidParameterValue(this.bindGroupList.size(), indexBasedZero);
        }
        final int firstGroupSize = this.firstGroupSize;
        if (firstGroupSize > -1 && indexBasedZero >= firstGroupSize) {
            throw MySQLExceptions.beyondFirstParamGroupRange(indexBasedZero, firstGroupSize);
        }
        return indexBasedZero;
    }

    private void clearStatementToAvoidReuse() {
        this.bindGroup = null;
    }


}
