package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * <p>
 * This class is base of below:
 *     <ul>
 *         <li>{@link ComQueryDataTypeSuiteTests}</li>
 *     </ul>
 * </p>
 */
abstract class AbstractDataTypeSuiteTests extends AbstractTaskSuiteTests {

    private static final Queue<ClientProtocol> PROTOCOL_QUEUE = new LinkedBlockingQueue<>();


    private final long startId;

    AbstractDataTypeSuiteTests(long startId) {
        this.startId = startId;
    }

    abstract Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant);

    abstract Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant);


    /**
     * @see MySQLType#TINYINT
     * @see MySQLType#TINYINT_UNSIGNED
     */
    final void tinyInt() {
        final long id = startId + 1;
        String column = "my_tinyint";
        MySQLType type = MySQLType.TINYINT;

        testType(id, column, type, null);

        testType(id, column, type, (byte) 0);

        testType(id, column, type, Byte.MIN_VALUE);
        testType(id, column, type, Byte.MAX_VALUE);
        testType(id, column, type, (short) Byte.MIN_VALUE);
        testType(id, column, type, (short) Byte.MAX_VALUE);

        testType(id, column, type, (int) Byte.MIN_VALUE);
        testType(id, column, type, (int) Byte.MAX_VALUE);
        testType(id, column, type, (long) Byte.MIN_VALUE);
        testType(id, column, type, (long) Byte.MAX_VALUE);

        testType(id, column, type, BigInteger.valueOf(Byte.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Byte.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Byte.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Byte.MAX_VALUE));


    }


    private ResultRow testType(final long id, final String column, final MySQLType type, @Nullable final Object value) {

        String sql;
        sql = String.format("UPDATE mysql_types AS t SET t.%s = ? WHERE t.id = ? ", column);
        final List<BindValue> bindGroup = new ArrayList<>(2);
        bindGroup.add(BindValue.wrap(0, type, value));
        bindGroup.add(BindValue.wrap(1, MySQLType.BIGINT, id));

        final BindStmt updateStmt, queryStmt;
        updateStmt = Stmts.bind(sql, bindGroup);
        sql = String.format("SELECT t.id,t.%s FROM mysql_types AS t WHERE t.id = ?", column);
        queryStmt = Stmts.single(sql, MySQLType.BIGINT, id);

        final ResultRow row;
        row = getClientProtocol()
                .flatMap(protocol -> executeStmt(protocol, updateStmt, queryStmt, value))
                .block();
        assertNotNull(row, "row");
        if (value == null) {
            assertNotNull(row.get(column));
        } else {
            assertResult(column, type, row, value);
        }
        return row;
    }

    private void assertResult(final String column, final MySQLType type, final ResultRow row, final Object nonNull) {

        switch (type) {
            case TINYINT:
            case TINYINT_UNSIGNED:
            default: {
                assertEquals(row.get(column, nonNull.getClass()), nonNull, column);
            }
        }


    }


    private Mono<ResultRow> executeStmt(final ClientProtocol protocol, final BindStmt updateStmt
            , final BindStmt queryStmt, @Nullable final Object value) {
        final TaskAdjutant adjutant = getTaskAdjutant(protocol);
        return executeUpdate(updateStmt, adjutant)
                .switchIfEmpty(Mono.defer(this::updateFailure))
                .thenMany(executeQuery(queryStmt, adjutant))

                .concatWith(closeProtocol(protocol))
                .onErrorResume(error -> closeProtocol(protocol).then(Mono.error(error)))

                .last();
    }


    private <T> Mono<T> closeProtocol(final ClientProtocol protocol) {
        return protocol.reset()
                .doOnSuccess(v -> PROTOCOL_QUEUE.offer(protocol))
                .then(Mono.empty());
    }


    private <T> Mono<T> updateFailure() {
        return Mono.error(new RuntimeException("update failure"));
    }

    private Mono<ClientProtocol> getClientProtocol() {
        final Mono<ClientProtocol> mono;
        ClientProtocol protocol;
        protocol = PROTOCOL_QUEUE.poll();
        if (protocol == null) {
            mono = ClientProtocolFactory.single(DEFAULT_SESSION_ADJUTANT);
        } else {
            mono = Mono.just(protocol);
        }
        return mono;
    }


}
