package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgNumbers;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * <p>
 * This class is base class of :
 * <u>
 * <li>{@link SimpleQuerySqlTypeSuiteTests}</li>
 * <li>{@link ExtendedQueryTaskSuiteTests}</li>
 * </u>
 * </p>
 */
abstract class AbstractStmtTaskTests extends AbstractTaskTests {

    private final int startId;

    AbstractStmtTaskTests(int startId) {
        this.startId = startId;
    }

    final void doInt2BindAndExtract() {
        final String columnName = "my_smallint";
        final long id = 1;

        Flux.just((short) 0, Short.MIN_VALUE, Short.MAX_VALUE, "1", 1)
                .flatMap(value -> updateColumn(columnName, PgType.SMALLINT, value, id))
                .map(PgNumbers::mapToShort)
                .flatMap(value -> queryColumn(columnName, PgType.SMALLINT, value, id))
                .then()
                .block();
    }

    abstract BiFunction<BindStmt, TaskAdjutant, Mono<ResultStates>> updateFunction();

    abstract BiFunction<BindStmt, TaskAdjutant, Flux<ResultRow>> queryFunction();


    private <T> Mono<T> updateColumn(String columnName, PgType columnType, @Nullable T value, long id) {
        final String sql = String.format("UPDATE my_types as t SET %s = ? WHERE t.id = ?", columnName);
        List<BindValue> paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, columnType, value));
        paramGroup.add(BindValue.create(1, PgType.BIGINT, id));

        return executeUpdate(PgStmts.bind(sql, paramGroup))
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .flatMap(state -> assertUpdateState(columnName, id, state, value))
                .then(Mono.justOrEmpty(value));
    }


    private <T> Mono<T> queryColumn(String columnName, PgType expectedType, @Nullable T value, long id) {
        final String sql = String.format("SELECT t.%s FROM my_types as t WHERE t.id = ?", columnName);
        return executeQuery(PgStmts.bind(sql, BindValue.create(0, PgType.BIGINT, id)))
                .switchIfEmpty(PgTestUtils.queryNoResponse())
                .map(row -> mapColumnValue(row, columnName, expectedType, id))
                .flatMap(columnValue -> assertColumnValue(columnValue, columnName, value, id))
                .next();
    }


    /**
     * @see #updateColumn(String, PgType, Object, long)
     */
    private <T> Mono<T> assertUpdateState(String columnName, long id, ResultStates state, @Nullable T value) {
        final Mono<T> mono;
        if (state.getAffectedRows() == 0L) {
            String m = String.format("column[%s] update failure,id[%s].", columnName, id);
            mono = Mono.error(new RuntimeException(m));
        } else {
            mono = Mono.justOrEmpty(value);
        }
        return mono;
    }

    /**
     * @see #queryColumn(String, PgType, Object, long)
     */
    private Object mapColumnValue(ResultRow row, String columnName, PgType expectedType, long id) {
        SQLType sqlType = row.getRowMeta().getSQLType(0);
        if (sqlType != expectedType) {
            String m = String.format("column[%s] sql type[%s] and¬ expected[%s] not match,id[%s]"
                    , columnName, sqlType, expectedType, id);
            throw new RuntimeException(m);
        }
        return row.getNonNull(0);
    }

    /**
     * @see #queryColumn(String, PgType, Object, long)
     */
    private <T> Mono<T> assertColumnValue(Object columnValue, String columnName, @Nullable T value, long id) {
        final Mono<T> mono;
        if (columnName.equals(value)) {
            mono = Mono.just(value);
        } else {
            String m = String.format("column[%s] value[%s] and value[%s] not equals,id[%s]"
                    , columnName, columnValue, value, id);
            mono = Mono.error(new RuntimeException(m));
        }
        return mono;
    }


    private Mono<ResultStates> executeUpdate(BindStmt stmt) {
        return obtainProtocol()
                .flatMap(protocol -> doExecuteUpdateAndClose(stmt, protocol));
    }

    private Mono<ResultStates> doExecuteUpdateAndClose(BindStmt stmt, ClientProtocol protocol) {
        return updateFunction()
                .apply(stmt, mapToTaskAdjutant(protocol))
                .concatWith(releaseConnection(protocol))
                .next();


    }

    private Flux<ResultRow> executeQuery(BindStmt stmt) {
        return obtainProtocol()
                .flatMapMany(protocol -> doExecuteQueryAndClose(stmt, protocol));
    }

    private Flux<ResultRow> doExecuteQueryAndClose(BindStmt stmt, ClientProtocol protocol) {
        return queryFunction()
                .apply(stmt, mapToTaskAdjutant(protocol))
                .concatWith(releaseConnection(protocol));
    }


}
