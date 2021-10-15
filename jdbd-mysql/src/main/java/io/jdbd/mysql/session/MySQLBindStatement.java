package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrBindStatement;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.QueryAttr;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.util.JdbdBinds;
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

    private final List<List<BindValue>> paramGroupList = new ArrayList<>();

    private List<BindValue> paramGroup = new ArrayList<>();

    private int firstGroupSize = -1;

    private Map<String, QueryAttr> attrGroup;

    public MySQLBindStatement(final MySQLDatabaseSession session, final String sql) {
        super(session);
        this.sql = sql;
    }

    @Override
    public void bind(final int indexBasedZero, final JDBCType jdbcType, @Nullable final Object nullable)
            throws JdbdException {
        final MySQLType type = MySQLBinds.mapJdbcTypeToMySQLType(jdbcType, nullable);
        this.paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), type, nullable));
    }

    @Override
    public void bind(final int indexBasedZero, final SQLType sqlType, @Nullable final Object nullable)
            throws JdbdException {
        if (!(sqlType instanceof MySQLType)) {
            String m = String.format("sqlType isn't a instance of %s", MySQLType.class.getName());
            throw new MySQLJdbdException(m);
        }
        this.paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), (MySQLType) sqlType, nullable));
    }

    @Override
    public void bind(final int indexBasedZero, @Nullable final Object nullable) throws JdbdException {
        this.paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), MySQLBinds.inferMySQLType(nullable), nullable));
    }

    @Override
    public void bindAttr(final String name, final MySQLType type, @Nullable final Object value) {
        checkReuse();
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
    public void addBatch() throws JdbdException {
        checkReuse();
        // add bind group
        final List<BindValue> paramGroup = this.paramGroup;
        int firstGroupSize = this.firstGroupSize;

        if (firstGroupSize < 0) {
            firstGroupSize = paramGroup.size();
            this.firstGroupSize = firstGroupSize;
        }

        if (paramGroup.size() != firstGroupSize) {
            throw MySQLExceptions.notMatchWithFirstParamGroupCount(this.paramGroupList.size()
                    , paramGroup.size(), firstGroupSize);
        } else {
            final JdbdException error = JdbdBinds.sortAndCheckParamGroup(this.paramGroupList.size(), paramGroup);
            if (error != null) {
                throw error;
            }
        }

        switch (paramGroup.size()) {
            case 0: {
                this.paramGroupList.add(Collections.emptyList());
            }
            break;
            case 1: {
                this.paramGroupList.add(Collections.singletonList(paramGroup.get(0)));
                this.paramGroup = new ArrayList<>(1);
            }
            break;
            default: {
                this.paramGroupList.add(Collections.unmodifiableList(paramGroup));
                this.paramGroup = new ArrayList<>(firstGroupSize);
            }
        }

        // add query attributes group
        addBatchQueryAttr(this.attrGroup);
        this.attrGroup = null;
    }

    @Override
    public Mono<ResultStates> executeUpdate() {
        final List<BindValue> paramGroup = this.paramGroup;
        final JdbdException error;
        if (paramGroup == null) {
            error = MySQLExceptions.cannotReuseStatement(BindStatement.class);
        } else if (this.paramGroupList.size() > 0 || attrGroupListNotEmpty()) {
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
            BindStmt stmt = Stmts.bind(this.sql, paramGroup, this.statementOption);
            mono = this.session.protocol.bindableUpdate(stmt);
        } else {
            mono = Mono.error(error);
        }
        return mono;
    }

    @Override
    public Flux<ResultRow> executeQuery() {
        return null;
    }

    @Override
    public Flux<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer) {
        return null;
    }

    @Override
    public Flux<ResultStates> executeBatch() {
        return null;
    }

    @Override
    public MultiResult executeBatchAsMulti() {
        return null;
    }

    @Override
    public OrderedFlux executeBatchAsFlux() {
        return null;
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
        if (this.paramGroup == null) {
            throw MySQLExceptions.cannotReuseStatement(BindStatement.class);
        }
    }

    /*################################## blow private method ##################################*/

    /**
     * @throws JdbdSQLException when indexBasedZero error
     */
    private int checkIndex(final int indexBasedZero) throws JdbdSQLException {
        if (indexBasedZero < 0) {
            throw MySQLExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
        }
        final int firstGroupSize = this.firstGroupSize;
        if (firstGroupSize > -1 && indexBasedZero >= firstGroupSize) {
            throw MySQLExceptions.beyondFirstParamGroupRange(indexBasedZero, firstGroupSize);
        }
        return indexBasedZero;
    }


}
