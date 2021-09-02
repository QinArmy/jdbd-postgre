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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;


/**
 * <p>
 * This class is a implementation of {@link PreparedStatement} with postgre client protocol.
 * </p>
 */
final class PgPreparedStatement extends PgStatement implements PreparedStatement {

    static PgPreparedStatement create(PgDatabaseSession session, PrepareStmtTask stmtTask) {
        return new PgPreparedStatement(session, stmtTask);
    }

    private final PrepareStmtTask stmtTask;

    private final List<PgType> paramTypeList;

    private final int paramCount;

    private final ResultRowMeta rowMeta;

    private final List<List<BindValue>> paramGroupList = new LinkedList<>();

    private List<BindValue> paramGroup;

    private int fetchSize;

    private PgPreparedStatement(PgDatabaseSession session, PrepareStmtTask stmtTask) {
        super(session);
        this.stmtTask = stmtTask;
        this.paramTypeList = stmtTask.getParamTypeList();
        this.paramCount = this.paramTypeList.size();
        this.rowMeta = stmtTask.getRowMeta();

        this.paramGroup = new ArrayList<>(this.paramCount);
    }


    @Override
    public final void bind(final int indexBasedZero, final JDBCType jdbcType, final @Nullable Object nullable)
            throws JdbdException {
        final PgType pgType;
        try {
            pgType = PgBinds.mapJdbcTypeToPgType(jdbcType, nullable);
        } catch (Throwable e) {
            final JdbdException error = PgExceptions.wrap(e);
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        this.paramGroup.add(BindValue.create(checkBindIndex(indexBasedZero), pgType, nullable));
    }

    @Override
    public final void bind(final int indexBasedZero, final SQLType sqlType, final @Nullable Object nullable)
            throws JdbdException {

        final JdbdException error;
        if (!(sqlType instanceof PgType)) {
            String m = String.format("sqlType isn't a instance of %s", PgType.class.getName());
            error = new PgJdbdException(m);
        } else {
            error = null;
        }

        if (error == null) {
            this.paramGroup.add(BindValue.create(checkBindIndex(indexBasedZero), (PgType) sqlType, nullable));
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }

    }

    @Override
    public final void bind(final int indexBasedZero, final @Nullable Object nullable)
            throws JdbdException {
        final PgType pgType = this.paramTypeList.get(checkBindIndex(indexBasedZero));
        this.paramGroup.add(BindValue.create(indexBasedZero, pgType, nullable));
    }


    @Override
    public final void addBatch() throws JdbdException {
        final List<BindValue> paramGroup = this.paramGroup;
        final int paramCount = this.paramCount;

        final JdbdException error;
        if (paramGroup.size() != paramCount) {
            error = PgExceptions.parameterCountMatch(this.paramGroupList.size()
                    , this.paramCount, paramGroup.size());
        } else {
            error = sortAndCheckParamGroup(this.paramGroupList.size(), paramGroup);
        }
        if (error == null) {
            this.paramGroupList.add(paramGroup);
            this.paramGroup = new ArrayList<>(this.paramCount);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }

    }


    @Override
    public final Mono<ResultStates> executeUpdate() {

        final List<BindValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (this.rowMeta != null) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.QUERY);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.UPDATE, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = PgExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = sortAndCheckParamGroup(0, paramGroup);
        }

        final Mono<ResultStates> mono;
        if (error == null) {
            BindStmt stmt = PgStmts.bind(this.stmtTask.getSql(), paramGroup, this);
            mono = this.stmtTask.executeUpdate(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
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
        Objects.requireNonNull(statesConsumer, "statesConsumer");

        final List<BindValue> paramGroup = this.paramGroup;

        final JdbdException error;
        if (this.rowMeta == null) {
            error = new SubscribeException(ResultType.QUERY, ResultType.UPDATE);
        } else if (!this.paramGroupList.isEmpty()) {
            error = new SubscribeException(ResultType.QUERY, ResultType.BATCH);
        } else if (paramGroup.size() != this.paramCount) {
            error = PgExceptions.parameterCountMatch(0, this.paramCount, paramGroup.size());
        } else {
            error = sortAndCheckParamGroup(0, paramGroup);
        }

        final Flux<ResultRow> flux;
        if (error == null) {
            BindStmt stmt = PgStmts.bind(this.stmtTask.getSql(), paramGroup, statesConsumer, this);
            flux = this.stmtTask.executeQuery(stmt);
        } else {
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        }
        return flux;
    }

    @Override
    public final Flux<ResultStates> executeBatch() {
        final Flux<ResultStates> flux;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        } else {
            BatchBindStmt stmt = PgStmts.bindableBatch(this.stmtTask.getSql(), this.paramGroupList, this);
            flux = this.stmtTask.executeBatch(stmt);
        }
        return flux;
    }

    @Override
    public final MultiResult executeBatchAsMulti() {
        final MultiResult result;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            result = MultiResults.error(error);
        } else {
            BatchBindStmt stmt = PgStmts.bindableBatch(this.stmtTask.getSql(), this.paramGroupList, this);
            result = this.stmtTask.executeBatchAsMulti(stmt);
        }
        return result;
    }

    @Override
    public final Flux<Result> executeBatchAsFlux() {
        final Flux<Result> flux;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = PgExceptions.noAnyParamGroupError();
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            flux = Flux.error(error);
        } else {
            BatchBindStmt stmt = PgStmts.bindableBatch(this.stmtTask.getSql(), this.paramGroupList, this);
            flux = this.stmtTask.executeBatchAsFlux(stmt);
        }
        return flux;
    }



    /*################################## blow Statement method ##################################*/

    @Override
    public final boolean supportPublisher() {
        return true;
    }


    @Override
    public final boolean setFetchSize(final int fetchSize) {
        final boolean support = this.rowMeta != null && fetchSize > 0;
        if (support) {
            this.fetchSize = fetchSize;
        } else {
            this.fetchSize = 0;
        }
        return support;
    }


    /*################################## blow StatementOption method ##################################*/

    @Override
    public final int getFetchSize() {
        return this.fetchSize;
    }

    /*################################## blow private method ##################################*/


    /**
     * <p>
     * If error ,this method invoke {@link PrepareStmtTask#closeOnBindError(Throwable)} before throw {@link Throwable}.
     * </p>
     *
     * @throws JdbdSQLException when indexBasedZero error
     */
    private int checkBindIndex(final int indexBasedZero) throws JdbdSQLException {
        if (indexBasedZero < 0 || indexBasedZero >= this.paramCount) {
            final JdbdException error = PgExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
            this.stmtTask.closeOnBindError(error); // close prepare statement.
            throw error;
        }
        return indexBasedZero;
    }


}
