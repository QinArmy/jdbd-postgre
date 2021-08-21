package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is test class of {@link SimpleQueryTask}.
 * </p>
 *
 * @see SimpleQueryTask
 */
//@Test(groups = {Group.SIMPLE_QUERY_TASK}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER, Group.TASK_TEST_ADVICE})
public class SimpleQueryTaskSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleQueryTaskSuiteTests.class);

    private static final long startId = 0;


    /**
     * @see SimpleQueryTask#update(Stmt, TaskAdjutant)
     */
    @Test
    public void update() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final long bindId = (startId + 1);
        final String sql = "UPDATE my_types as t SET my_boolean = true WHERE t.id = " + bindId;
        ResultState state;
        state = SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)
                .concatWith(releaseConnection(protocol))
                .next()
                .onErrorResume(releaseConnectionOnError(protocol))
                .block();

        assertNotNull(state, "state");

        assertEquals(state.getAffectedRows(), 1L, "rows");
        assertEquals(state.getInsertId(), 0L, "insert id");
        assertEquals(state.getResultIndex(), 0, "resultIndex");
        assertFalse(state.hasMoreFetch(), "moreFetch");

        assertFalse(state.hasMoreResult(), "moreResult");
        assertFalse(state.hasReturningColumn(), "hasReturningColumn");

    }

    /**
     * @see SimpleQueryTask#query(Stmt, TaskAdjutant)
     */
    @Test
    public void query() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final long bindId = (startId + 2);
        final String sql = "SELECT t.* FROM my_types as t WHERE t.id = " + bindId;

        final AtomicReference<ResultState> stateHolder = new AtomicReference<>(null);
        final ResultRow row;
        row = SimpleQueryTask.query(PgStmts.stmt(sql, stateHolder::getAndSet), adjutant)
                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))
                .blockLast();

        assertNotNull(row, "row");

        final ResultRowMeta rowMeta = row.getRowMeta();
        assertEquals(rowMeta.getSQLType("id"), PgType.BIGINT, "id sql type");
        assertEquals(row.get("id"), bindId, "id");
        assertNotNull(row.get("my_zoned_timestamp"), "my_zoned_timestamp");

        final ResultState state = stateHolder.get();

        assertNotNull(state, "ResultState");
        assertEquals(state.getAffectedRows(), 0L, "rows");
        assertEquals(state.getInsertId(), 0L, "insert id");
        assertEquals(state.getResultIndex(), 0, "resultIndex");
        assertFalse(state.hasMoreFetch(), "moreFetch");

        assertFalse(state.hasMoreResult(), "moreResult");
        assertTrue(state.hasReturningColumn(), "hasReturningColumn");
    }

    /**
     * @see SimpleQueryTask#batchUpdate(GroupStmt, TaskAdjutant)
     */
    @Test
    public void batchUpdate() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        List<String> sqlList = new ArrayList<>(2);
        long bindId = startId + 3;
        sqlList.add("UPDATE my_types as t SET my_boolean = true WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types as t SET my_boolean = false WHERE t.id = " + bindId);

        final List<ResultState> stateList;
        stateList = SimpleQueryTask.batchUpdate(PgStmts.group(sqlList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(releaseConnection(protocol))
                .collectList()
                .onErrorResume(releaseConnectionOnError(protocol))
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), sqlList.size(), "stateList size");

        final int size = sqlList.size(), last = size - 1;
        for (int i = 0; i < size; i++) {
            ResultState state = stateList.get(i);

            assertEquals(state.getAffectedRows(), 1L, "rows");
            assertEquals(state.getInsertId(), 0L, "insert id");
            assertEquals(state.getResultIndex(), i, "resultIndex");
            assertFalse(state.hasMoreFetch(), "moreFetch");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "moreResult");
            } else {
                assertTrue(state.hasMoreResult(), "moreResult");
            }
            assertFalse(state.hasReturningColumn(), "hasReturningColumn");
        }


    }

    /**
     * <p>
     *
     * </p>
     *
     * @see SimpleQueryTask#bindableUpdate(BindableStmt, TaskAdjutant)
     */
    @Test
    public void bindableUpdate() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long bindId = startId + 6;

        final String sql = "UPDATE my_types as t SET my_time =? WHERE t.id = ?";
        final List<BindValue> valueList = new ArrayList<>(2);
        valueList.add(BindValue.create(0, PgType.TIME, LocalTime.now()));
        valueList.add(BindValue.create(1, PgType.BIGINT, bindId));

        final ResultState state;
        state = SimpleQueryTask.bindableUpdate(PgStmts.bindable(sql, valueList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(releaseConnection(protocol))
                .collectList()
                .map(PgTestUtils::mapListToOne)
                .onErrorResume(releaseConnectionOnError(protocol))
                .block();

        assertNotNull(state, "ResultState");
        assertEquals(state.getAffectedRows(), 1L, "rows");
        assertEquals(state.getInsertId(), 0L, "insert id");
        assertEquals(state.getResultIndex(), 0, "resultIndex");
        assertFalse(state.hasMoreFetch(), "moreFetch");

        assertFalse(state.hasMoreResult(), "moreResult");
        assertFalse(state.hasReturningColumn(), "hasReturningColumn");

    }

    /**
     * @see SimpleQueryTask#asMulti(GroupStmt, TaskAdjutant)
     */
    @Test
    public void asMulti() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        String sql;
        long bindId = startId + 8;
        List<String> sqlList = new ArrayList<>(4);
        sqlList.add(String.format("UPDATE my_types AS t SET my_time = '%s' WHERE t.id = %s", LocalTime.now().format(PgTimes.ISO_LOCAL_TIME_FORMATTER), bindId++));
        sqlList.add("SELECT t.* FROM my_types AS t WHERE t.id = " + (bindId++));
        sqlList.add(String.format("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = %s RETURNING t.id AS id ", bindId++));
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId);

        final MultiResult multiResult = SimpleQueryTask.asMulti(PgStmts.group(sqlList), adjutant);

        final AtomicReference<ResultState> updateRowsHolder = new AtomicReference<>(null);

        Mono.from(multiResult.nextUpdate())
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .map(PgTestUtils::assertUpdateOneWithMoreResult)

                .thenMany(multiResult.nextQuery())
                .switchIfEmpty(PgTestUtils.queryNoResponse())

                .thenMany(multiResult.nextQuery(updateRowsHolder::set))
                .switchIfEmpty(PgTestUtils.queryNoResponse())

                .then(Mono.from(multiResult.nextUpdate()))
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .map(PgTestUtils::assertUpdateOneWithoutMoreResult)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))
                .then()
                .block();

        final ResultState state = updateRowsHolder.get();
        // assertNotNull(state, "state");
        //PgTestUtils.assertUpdateOneAndReturningWithMoreResult(state);
    }

    /**
     * @see SimpleQueryTask#bindableQuery(BindableStmt, TaskAdjutant)
     */
    @Test
    public void bindableQuery() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long id = 7L;
        // Postgre server 12.6 bug ,RETURNING clause output_name is converted to lower case by server,so 'mytime' not 'myTime'.
        final String sql = "UPDATE my_types as t SET my_time =? WHERE t.id = ? RETURNING t.id AS id,t.my_time AS mytime";
        final LocalTime time = LocalTime.now();
        final List<BindValue> valueList = new ArrayList<>(2);
        valueList.add(BindValue.create(0, PgType.TIME, time));
        valueList.add(BindValue.create(1, PgType.BIGINT, id));

        final ResultRow row;
        row = SimpleQueryTask.bindableQuery(PgStmts.bindable(sql, valueList), adjutant)
                .switchIfEmpty(PgTestUtils.queryNoResponse())
                .concatWith(releaseConnection(protocol))
                .collectList()
                .map(PgTestUtils::mapListToOne)
                .onErrorResume(releaseConnectionOnError(protocol))
                .block();

        assertNotNull(row, "row");
        final long rowId = row.getNonNull("id", Long.class);
        assertEquals(rowId, id, "row id");
        final LocalTime myTime = row.getNonNull("mytime", LocalTime.class);

        assertEquals(myTime.getHour(), time.getHour(), "hour");
        assertEquals(myTime.getMinute(), time.getMinute(), "minute");
        assertEquals(myTime.getSecond(), time.getSecond(), "second");

        assertEquals(myTime.getLong(ChronoField.MICRO_OF_SECOND), time.getLong(ChronoField.MICRO_OF_SECOND), "miro second");
    }


}
