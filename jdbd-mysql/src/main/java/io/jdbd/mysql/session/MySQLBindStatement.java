package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.*;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.util.JdbdBinds;
import io.jdbd.vendor.util.JdbdFunctions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.*;
import java.util.function.Consumer;

/**
 * <p>
 * This interface is a implementation of {@link io.jdbd.stmt.BindStatement} with MySQL client protocol.
 * </p>
 */
final class MySQLBindStatement extends MySQLStatement implements AttrBindStatement {

    static MySQLBindStatement create(final MySQLDatabaseSession session, final String sql) {
        return new MySQLBindStatement(session, sql);
    }

    private final String sql;

    private final List<List<BindValue>> bindGroupList = new ArrayList<>();

    private List<BindValue> bindGroup = new ArrayList<>();

    private int firstGroupSize = -1;

    private Map<String, QueryAttr> attrGroup;

    public MySQLBindStatement(final MySQLDatabaseSession session, final String sql) {
        super(session);
        this.sql = sql;
    }

    @Override
    public void bind(final int indexBasedZero, final JDBCType jdbcType, @Nullable final Object nullable)
            throws JdbdException {
        checkReuse();
        final MySQLType type = MySQLBinds.mapJdbcTypeToMySQLType(jdbcType, nullable);
        this.bindGroup.add(BindValue.wrap(checkIndex(indexBasedZero), type, nullable));
    }

    @Override
    public void bind(final int indexBasedZero, final SQLType sqlType, @Nullable final Object nullable)
            throws JdbdException {
        checkReuse();
        if (!(sqlType instanceof MySQLType)) {
            String m = String.format("sqlType isn't a instance of %s", MySQLType.class.getName());
            throw new MySQLJdbdException(m);
        }
        this.bindGroup.add(BindValue.wrap(checkIndex(indexBasedZero), (MySQLType) sqlType, nullable));
    }

    @Override
    public void bind(final int indexBasedZero, @Nullable final Object nullable) throws JdbdException {
        checkReuse();
        this.bindGroup.add(BindValue.wrap(checkIndex(indexBasedZero), MySQLBinds.inferMySQLType(nullable), nullable));
    }

    @Override
    public void bindAttr(final String name, final MySQLType type, @Nullable final Object value) {
        checkReuse();
        Objects.requireNonNull(name, "name");
        Map<String, QueryAttr> attrGroup = this.attrGroup;
        if (attrGroup == null) {
            attrGroup = new HashMap<>();
            this.attrGroup = attrGroup;
            prepareAttrGroupList(this.bindGroupList.size());
        }
        attrGroup.put(name, QueryAttr.wrap(type, value));
    }

    @Override
    public void addBatch() throws JdbdException {
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
                this.bindGroup = new ArrayList<>(1);
            }
            break;
            default: {
                this.bindGroupList.add(Collections.unmodifiableList(paramGroup));
                this.bindGroup = new ArrayList<>(firstGroupSize);
            }
        }

        // add query attributes group
        addBatchQueryAttr(this.attrGroup);
        this.attrGroup = null;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        final List<BindValue> paramGroup = this.bindGroup;
        final JdbdException error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.bindGroupList.size() > 0 || attrGroupListNotEmpty()) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH);
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
            final Map<String, QueryAttr> attrGroup = this.attrGroup;
            if (error == null && attrGroup != null) {
                prepareAttrGroup(attrGroup);
            }
        }
        final Mono<ResultStates> mono;
        if (error == null) {
            this.statementOption.fetchSize = 0;
            final BindStmt stmt = Stmts.bind(this.sql, paramGroup, this.statementOption);
            mono = this.session.protocol.bindUpdate(stmt);
        } else {
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

        final List<BindValue> paramGroup = this.bindGroup;
        final JdbdException error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.bindGroupList.size() > 0 || attrGroupListNotEmpty()) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH);
        } else {
            error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
            final Map<String, QueryAttr> attrGroup = this.attrGroup;
            if (error == null && attrGroup != null) {
                prepareAttrGroup(attrGroup);
            }
        }
        final Flux<ResultRow> flux;
        if (error == null) {
            final BindStmt stmt = Stmts.bind(this.sql, paramGroup, statesConsumer, this.statementOption);
            flux = this.session.protocol.bindQuery(stmt);
        } else {
            flux = Flux.error(error);
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public Flux<ResultStates> executeBatch() {
        final Flux<ResultStates> flux;
        final int batchCount = this.bindGroupList.size();
        if (this.bindGroup == null) {
            flux = Flux.error(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (batchCount == 0) {
            flux = Flux.error(MySQLExceptions.noAnyParamGroupError());
        } else {
            final Throwable e;
            e = checkBatchAttrGroupListSize(batchCount);
            if (e == null) {
                this.statementOption.fetchSize = 0; // executeBatch() don't support fetch.
                final BindBatchStmt stmt = Stmts.bindBatch(this.sql, this.bindGroupList, this.statementOption);
                flux = this.session.protocol.bindBatch(stmt);
            } else {
                flux = Flux.error(e);
            }
        }
        clearStatementToAvoidReuse();
        return flux;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        final MultiResult result;
        final int batchCount = this.bindGroupList.size();
        if (this.bindGroup == null) {
            result = MultiResults.error(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (batchCount == 0) {
            result = MultiResults.error(MySQLExceptions.noAnyParamGroupError());
        } else {
            final Throwable e;
            e = checkBatchAttrGroupListSize(batchCount);
            if (e == null) {
                this.statementOption.fetchSize = 0; // executeBatchAsMulti() don't support fetch.
                final BindBatchStmt stmt = Stmts.bindBatch(this.sql, this.bindGroupList, this.statementOption);
                result = this.session.protocol.bindBatchAsMulti(stmt);
            } else {
                result = MultiResults.error(e);
            }
        }
        clearStatementToAvoidReuse();
        return result;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        final OrderedFlux flux;
        final IllegalStateException error;
        final int batchCount = this.bindGroupList.size();
        if (this.bindGroup == null) {
            flux = MultiResults.fluxError(MySQLExceptions.cannotReuseStatement(BindStatement.class));
        } else if (batchCount == 0) {
            flux = MultiResults.fluxError(MySQLExceptions.noAnyParamGroupError());
        } else if ((error = checkBatchAttrGroupListSize(batchCount)) != null) {
            flux = MultiResults.fluxError(error);
        } else {
            final BindBatchStmt stmt = Stmts.bindBatch(this.sql, this.bindGroupList, this.statementOption);
            flux = this.session.protocol.bindBatchAsFlux(stmt);
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
        this.attrGroup = null;
    }


}
