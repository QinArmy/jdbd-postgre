package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindBatchStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.result.MultiResults;
import io.jdbd.vendor.util.JdbdBinds;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.sql.JDBCType;
import java.util.*;
import java.util.function.Consumer;


/**
 * <p>
 * This class is a implementation of {@link BindStatement} with postgre client protocol.
 * </p>
 *
 * @see PgDatabaseSession#bindable(String)
 */
final class PgBindStatement extends PgStatement implements BindStatement {

    /**
     * @see PgDatabaseSession#bindable(String)
     */
    static PgBindStatement create(String sql, PgDatabaseSession session) {
        if (!PgStrings.hasText(sql)) {
            throw new IllegalArgumentException("sql must be have text.");
        }
        return new PgBindStatement(sql, session);
    }

    private final String sql;

    private final List<List<BindValue>> paramGroupList = new LinkedList<>();

    private List<BindValue> paramGroup = new ArrayList<>();

    private int firstGroupSize = -1;

    private int fetchSize = 0;


    private PgBindStatement(String sql, PgDatabaseSession session) {
        super(session);
        this.sql = sql;
    }

    @Override
    public final void bind(final int indexBasedZero, final JDBCType jdbcType, final @Nullable Object nullable)
            throws JdbdException {
        final PgType pgType = PgBinds.mapJdbcTypeToPgType(jdbcType, nullable);
        this.paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), pgType, nullable));
    }

    @Override
    public final void bind(final int indexBasedZero, final SQLType sqlType, final @Nullable Object nullable)
            throws JdbdException {

        if (!(sqlType instanceof PgType)) {
            String m = String.format("sqlType isn't a instance of %s", PgType.class.getName());
            throw new PgJdbdException(m);
        }
        this.paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), (PgType) sqlType, nullable));
    }

    @Override
    public final void bind(final int indexBasedZero, final @Nullable Object nullable)
            throws JdbdException {
        this.paramGroup.add(BindValue.wrap(checkIndex(indexBasedZero), PgBinds.inferPgType(nullable), nullable));
    }

    @Override
    public final void addBatch() throws JdbdException {
        final List<BindValue> paramGroup = this.paramGroup;
        int firstGroupSize = this.firstGroupSize;

        if (firstGroupSize < 0) {
            firstGroupSize = paramGroup.size();
            this.firstGroupSize = firstGroupSize;
        }

        if (paramGroup.size() != firstGroupSize) {
            throw PgExceptions.notMatchWithFirstParamGroupCount(this.paramGroupList.size()
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

    }

    @Override
    public final Mono<ResultStates> executeUpdate() {
        final List<BindValue> paramGroup = this.paramGroup;

        final Mono<ResultStates> mono;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
            if (error != null) {
                throw error;
            }
            BindStmt stmt = PgStmts.bind(this.sql, paramGroup, this);
            mono = this.session.protocol.bindUpdate(stmt);
        } else {
            mono = Mono.error(new SubscribeException(ResultType.UPDATE, ResultType.BATCH));
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

        final Flux<ResultRow> flux;
        if (this.paramGroupList.isEmpty()) {
            final JdbdException error = JdbdBinds.sortAndCheckParamGroup(0, paramGroup);
            if (error != null) {
                throw error;
            }
            BindStmt stmt = PgStmts.bind(this.sql, paramGroup, statesConsumer, this);
            flux = this.session.protocol.bindQuery(stmt);
        } else {
            flux = Flux.error(new SubscribeException(ResultType.QUERY, ResultType.BATCH));
        }
        return flux;
    }

    @Override
    public final Flux<ResultStates> executeBatch() {
        final Flux<ResultStates> flux;
        if (this.paramGroupList.isEmpty()) {
            flux = Flux.error(PgExceptions.noAnyParamGroupError());
        } else {
            BindBatchStmt stmt = PgStmts.bindableBatch(this.sql, this.paramGroupList, this);
            flux = this.session.protocol.bindBatch(stmt);
        }
        return flux;
    }

    @Override
    public final MultiResult executeBatchAsMulti() {
        final MultiResult result;
        if (this.paramGroupList.isEmpty()) {
            result = MultiResults.error(PgExceptions.noAnyParamGroupError());
        } else {
            BindBatchStmt stmt = PgStmts.bindableBatch(this.sql, this.paramGroupList, this);
            result = this.session.protocol.bindBatchAsMulti(stmt);
        }
        return result;
    }

    @Override
    public final OrderedFlux executeBatchAsFlux() {
        final OrderedFlux flux;
        if (this.paramGroupList.isEmpty()) {
            flux = MultiResults.orderedFluxError(PgExceptions.noAnyParamGroupError());
        } else {
            BindBatchStmt stmt = PgStmts.bindableBatch(this.sql, this.paramGroupList, this);
            flux = this.session.protocol.bindBatchAsFlux(stmt);
        }
        return flux;
    }

    /*################################## blow Statement method ##################################*/

    @Override
    public final boolean setFetchSize(final int fetchSize) {
        this.fetchSize = fetchSize;
        return fetchSize > 0;
    }

    @Override
    public final boolean supportPublisher() {
        return true;
    }

    @Override
    public final int getFetchSize() {
        return this.fetchSize;
    }

    /*################################## blow private method ##################################*/

    /**
     * @throws JdbdSQLException when indexBasedZero error
     */
    private int checkIndex(final int indexBasedZero) throws JdbdSQLException {
        if (indexBasedZero < 0) {
            throw PgExceptions.invalidParameterValue(this.paramGroupList.size(), indexBasedZero);
        }
        final int firstGroupSize = this.firstGroupSize;
        if (firstGroupSize > -1 && indexBasedZero >= firstGroupSize) {
            throw PgExceptions.beyondFirstParamGroupRange(indexBasedZero, firstGroupSize);
        }
        return indexBasedZero;
    }


}
