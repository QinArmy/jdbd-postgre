package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.*;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.result.*;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 *
 */
final class PgPreparedStatement extends PgStatement implements PreparedStatement {

    static PgPreparedStatement create(PgDatabaseSession session, PrepareStmtTask stmtTask) {
        return new PgPreparedStatement(session, stmtTask);
    }

    private final PrepareStmtTask stmtTask;

    private final List<Integer> paramOidList;

    private final int paramCount;

    private final ResultRowMeta rowMeta;

    private final List<List<BindValue>> paramGroupList = new LinkedList<>();

    private List<BindValue> paramGroup;

    private int fetchSize;

    private PgPreparedStatement(PgDatabaseSession session, PrepareStmtTask stmtTask) {
        super(session);
        this.stmtTask = stmtTask;
        this.paramOidList = stmtTask.getParamTypeOidList();
        this.paramCount = this.paramOidList.size();
        this.rowMeta = stmtTask.getRowMeta();

        this.paramGroup = new ArrayList<>(this.paramCount);
    }


    @Override
    public final void bind(final int indexBasedZero, final JDBCType jdbcType, final @Nullable Object nullable)
            throws JdbdException {
        bind(indexBasedZero, PgBinds.mapJdbcTypeToPgType(jdbcType, nullable), nullable);
    }

    @Override
    public final void bind(final int indexBasedZero, final SQLType sqlType, final @Nullable Object nullable)
            throws JdbdException {
        if (!(sqlType instanceof PgType)) {
            String m = String.format("sqlType isn't a instance of %s", PgType.class.getName());
            throw new PgJdbdException(m);
        }
        if (indexBasedZero < 0 || indexBasedZero >= this.paramCount) {
            SQLException e;
            e = PgExceptions.createInvalidParameterValueError(this.paramGroupList.size(), indexBasedZero);
            throw new JdbdSQLException(e);
        }
        this.paramGroup.add(BindValue.create(indexBasedZero, (PgType) sqlType, nullable));
    }

    @Override
    public final void bind(final int indexBasedZero, final @Nullable Object nullable)
            throws JdbdException {
        if (indexBasedZero < 0 || indexBasedZero >= this.paramCount) {
            SQLException e;
            e = PgExceptions.createInvalidParameterValueError(this.paramGroupList.size(), indexBasedZero);
            throw new JdbdSQLException(e);
        }
        bind(indexBasedZero, PgType.from(this.paramOidList.get(indexBasedZero)), nullable);
    }


    @Override
    public final void addBatch() throws JdbdException {
        final List<BindValue> paramGroup = this.paramGroup;
        final int paramCount = this.paramCount;
        if (paramGroup.size() != paramCount) {
            SQLException e;
            e = PgExceptions.createParameterCountMatchError(this.paramGroupList.size()
                    , this.paramCount, paramGroup.size());
            throw new JdbdSQLException(e);
        }
        final JdbdSQLException error;
        error = sortAndCheckParamGroup(paramGroup);
        if (error != null) {
            throw error;
        }
        this.paramGroupList.add(paramGroup);
        this.paramGroup = new ArrayList<>(this.paramCount);

    }


    @Override
    public final Mono<ResultStates> executeUpdate() {

        final List<BindValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH_UPDATE);
        } else if (paramGroup.size() != this.paramCount) {
            SQLException e = PgExceptions.createParameterCountMatchError(0, this.paramCount, paramGroup.size());
            error = new JdbdSQLException(e);
        } else {
            error = sortAndCheckParamGroup(paramGroup);
        }

        final Mono<ResultStates> mono;
        if (error == null) {
            BindStmt stmt = PgStmts.bindable(this.stmtTask.getSql(), paramGroup, this);
            mono = this.session.protocol.bindableUpdate(stmt);
        } else {
            this.stmtTask.closeOnBindError(); // close prepare statement.
            mono = Mono.error(error);
        }
        return mono;
    }

    @Override
    public final Flux<ResultRow> executeQuery() {
        return executeQuery(PgFunctions.noActionConsumer());
    }

    @Override
    public final Flux<ResultRow> executeQuery(final Consumer<ResultStates> statesConsumer) {

        final List<BindValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH_UPDATE);
        } else if (paramGroup.size() != this.paramCount) {
            SQLException e = PgExceptions.createParameterCountMatchError(0, this.paramCount, paramGroup.size());
            error = new JdbdSQLException(e);
        } else {
            error = sortAndCheckParamGroup(paramGroup);
        }

        final Flux<ResultRow> flux;
        if (error == null) {
            BindStmt stmt = PgStmts.bindable(this.stmtTask.getSql(), paramGroup, statesConsumer, this);
            flux = this.session.protocol.bindableQuery(stmt);
        } else {
            this.stmtTask.closeOnBindError(); // close prepare statement.
            flux = Flux.error(error);
        }
        return flux;
    }

    @Override
    public final Flux<ResultStates> executeBatch() {
        final Flux<ResultStates> flux;
        if (this.paramGroupList.isEmpty()) {
            this.stmtTask.closeOnBindError(); // close prepare statement.
            flux = Flux.error(new JdbdSQLException(PgExceptions.createNoAnyParamGroupError()));
        } else {
            BatchBindStmt stmt = PgStmts.bindableBatch(this.stmtTask.getSql(), this.paramGroupList, this);
            flux = this.session.protocol.bindableBatchUpdate(stmt);
        }
        return flux;
    }

    @Override
    public final MultiResult executeBatchAsMulti() {
        final MultiResult result;
        if (this.paramGroupList.isEmpty()) {
            this.stmtTask.closeOnBindError(); // close prepare statement.
            result = MultiResults.error(new JdbdSQLException(PgExceptions.createNoAnyParamGroupError()));
        } else {
            BatchBindStmt stmt = PgStmts.bindableBatch(this.stmtTask.getSql(), this.paramGroupList, this);
            result = this.session.protocol.bindableAsMulti(stmt);
        }
        return result;
    }

    @Override
    public final Flux<Result> executeBatchAsFlux() {
        final Flux<Result> flux;
        if (this.paramGroupList.isEmpty()) {
            this.stmtTask.closeOnBindError(); // close prepare statement.
            flux = Flux.error(new JdbdSQLException(PgExceptions.createNoAnyParamGroupError()));
        } else {
            BatchBindStmt stmt = PgStmts.bindableBatch(this.stmtTask.getSql(), this.paramGroupList, this);
            flux = this.session.protocol.bindableAsFlux(stmt);
        }
        return flux;
    }



    /*################################## blow Statement method ##################################*/

    @Override
    public final boolean supportLongData() {
        return true;
    }


    @Override
    public final boolean setFetchSize(int fetchSize) {
        final boolean support = this.rowMeta != null;
        if (support) {
            this.fetchSize = fetchSize;

        }
        return support;
    }


    /*################################## blow StatementOption method ##################################*/

    @Override
    public final int getFetchSize() {
        return this.fetchSize;
    }

    /*################################## blow private method ##################################*/

    @Nullable
    private JdbdSQLException sortAndCheckParamGroup(List<BindValue> paramGroup) throws JdbdSQLException {
        paramGroup.sort(Comparator.comparingInt(BindValue::getParamIndex));
        JdbdSQLException error = null;
        for (int i = 0, index; i < this.paramCount; i++) {
            index = paramGroup.get(i).getParamIndex();
            if (index == i) {
                continue;
            }
            SQLException e;
            if (index < i) {
                e = PgExceptions.createDuplicationParameterError(this.paramGroupList.size(), index);
            } else {
                e = PgExceptions.createNoParameterValueError(this.paramGroupList.size(), i);
            }
            error = new JdbdSQLException(e);
            break;
        }
        return error;
    }


}
