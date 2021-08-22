package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.*;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
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

    private static final long START_ID = 0;


    /**
     * @see SimpleQueryTask#update(Stmt, TaskAdjutant)
     */
    @Test
    public void update() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final long bindId = (START_ID + 1);
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
        final long bindId = (START_ID + 10);
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
        long bindId = START_ID + 20;
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
     * @see SimpleQueryTask#asMulti(GroupStmt, TaskAdjutant)
     */
    @Test
    public void asMulti() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 30;
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
        assertNotNull(state, "state");
        PgTestUtils.assertUpdateOneAndReturningWithMoreResult(state);
    }

    /**
     * @see SimpleQueryTask#asFlux(GroupStmt, TaskAdjutant)
     */
    @Test
    public void asFlux() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final long bindStartId = START_ID + 40;
        long bindId = bindStartId;

        List<String> sqlList = new ArrayList<>(4);
        sqlList.add(String.format("UPDATE my_types AS t SET my_time = '%s' WHERE t.id = %s", LocalTime.now().format(PgTimes.ISO_LOCAL_TIME_FORMATTER), bindId++));
        sqlList.add("SELECT t.* FROM my_types AS t WHERE t.id = " + (bindId++));
        sqlList.add(String.format("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = %s RETURNING t.id AS id ", bindId++));
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId);

        final List<Result> resultList;
        resultList = SimpleQueryTask.asFlux(PgStmts.group(sqlList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(resultList, "resultList");
        assertEquals(resultList.size(), 6, "resultList size");
        Result result;
        ResultState state;
        ResultRow row;

        // result 0
        result = resultList.get(0);
        assertEquals(result.getResultIndex(), 0, "result index"); // result index must be 0
        assertTrue(result instanceof ResultState, "first update statement.");
        state = (ResultState) result;

        assertFalse(state.hasReturningColumn(), "first update statement has returning column.");
        assertFalse(state.hasMoreFetch(), "first update statement more fetch.");
        assertEquals(state.getAffectedRows(), 1L, "first update statement affected rows");
        assertEquals(state.getInsertId(), 0L, "first update statement insert id");

        assertTrue(state.hasMoreResult(), "first update statement more result.");


        // result 1
        result = resultList.get(1);
        assertEquals(result.getResultIndex(), 1, "result index");// result index must be 1
        assertTrue(result instanceof ResultRow, "second select statement.");
        row = (ResultRow) result;

        assertEquals(row.get("id", Long.class), Long.valueOf(bindStartId + 1));
        result = resultList.get(2);
        assertEquals(result.getResultIndex(), 1, "result index");// result index must be 1
        assertTrue(result instanceof ResultState, "second select statement.");

        state = (ResultState) result;
        assertTrue(state.hasReturningColumn(), "second select statement has returning column.");
        assertFalse(state.hasMoreFetch(), "second select statement more fetch.");
        assertEquals(state.getAffectedRows(), 0L, "second select statement affected rows");

        assertEquals(state.getInsertId(), 0L, "second select statement insert id");
        assertTrue(state.hasMoreResult(), "second select statement more result.");

        // result 2
        result = resultList.get(3);
        assertEquals(result.getResultIndex(), 2, "result index");// result index must be 2
        assertTrue(result instanceof ResultRow, "third select statement.");
        row = (ResultRow) result;

        assertEquals(row.get("id", Long.class), Long.valueOf(bindStartId + 2));
        result = resultList.get(4);
        assertEquals(result.getResultIndex(), 2, "result index");// result index must be 2
        assertTrue(result instanceof ResultState, "third update returning statement.");

        state = (ResultState) result;
        assertTrue(state.hasReturningColumn(), "third update returning statement has returning column.");
        assertFalse(state.hasMoreFetch(), "third update returning statement more fetch.");
        assertEquals(state.getAffectedRows(), 1L, "third update returning statement affected rows");

        assertEquals(state.getInsertId(), 0L, "third update returning statement insert id");
        assertTrue(state.hasMoreResult(), "third update returning statement more result.");

        // result 3
        result = resultList.get(5);
        assertEquals(result.getResultIndex(), 3, "result index");// result index must be 3
        assertTrue(result instanceof ResultState, "fourth update statement.");
        state = (ResultState) result;

        assertFalse(state.hasReturningColumn(), "fourth update statement has returning column.");
        assertFalse(state.hasMoreFetch(), "fourth update statement more fetch.");
        assertEquals(state.getAffectedRows(), 1L, "fourth update statement affected rows");
        assertEquals(state.getInsertId(), 0L, "fourth update statement insert id");

        assertFalse(state.hasMoreResult(), "fourth update statement more result.");

    }

    /*################################## blow bindable method ##################################*/


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

        final long bindId = START_ID + 50;

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
     * @see SimpleQueryTask#bindableQuery(BindableStmt, TaskAdjutant)
     */
    @Test
    public void bindableQuery() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long id = START_ID + 60;
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
        // my_time precision is 6 ,so below assert
        assertEquals(myTime.getLong(ChronoField.MICRO_OF_SECOND)
                , time.getLong(ChronoField.MICRO_OF_SECOND), "micro second");

    }

    /**
     * @see SimpleQueryTask#bindableBatchUpdate(BatchBindStmt, TaskAdjutant)
     */
    @Test
    public void bindableBatchUpdate() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        long bindId = START_ID + 70;

        final String sql = "UPDATE my_types as t SET my_integer = ? WHERE t.id = ?";
        final int[] valueArray = new int[]{0, 999, Integer.MAX_VALUE, Integer.MIN_VALUE};
        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (int intValue : valueArray) {
            final List<BindValue> valueList = new ArrayList<>(2);
            valueList.add(BindValue.create(0, PgType.INTEGER, intValue));
            valueList.add(BindValue.create(1, PgType.BIGINT, bindId++));
            groupList.add(Collections.unmodifiableList(valueList));
        }

        final List<ResultState> stateList;
        stateList = SimpleQueryTask.bindableBatchUpdate(PgStmts.bindableBatch(sql, groupList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))
                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), valueArray.length, "stateList size");

        for (int i = 0, last = valueArray.length - 1; i < valueArray.length; i++) {
            ResultState state = stateList.get(i);
            assertEquals(state.getResultIndex(), i);
            assertEquals(state.getAffectedRows(), 1L, "getAffectedRows");
            assertEquals(state.getInsertId(), 0L, "insert id");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasReturningColumn(), "returning column");

        }

    }

    /**
     * @see SimpleQueryTask#bindableAsMulti(BatchBindStmt, TaskAdjutant)
     */
    @Test
    public void bindableAsMultiForUpdate() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 80;

        final String sql = "UPDATE my_types as t SET my_time = ?,my_boolean = TRUE WHERE t.id = ?";

        final LocalTime[] valueArray = new LocalTime[]{LocalTime.MIN, LocalTime.MAX, LocalTime.NOON, LocalTime.now()};

        List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (LocalTime localTime : valueArray) {
            final List<BindValue> valueList = new ArrayList<>(2);
            valueList.add(BindValue.create(0, PgType.TIME, localTime));
            valueList.add(BindValue.create(1, PgType.BIGINT, bindId++));
            groupList.add(Collections.unmodifiableList(valueList));
        }

        final MultiResult multiResult;
        multiResult = SimpleQueryTask.bindableAsMulti(PgStmts.bindableBatch(sql, groupList), adjutant);

        final List<ResultState> stateList;

        stateList = Mono.from(multiResult.nextUpdate())
                .concatWith(multiResult.nextUpdate())
                .concatWith(multiResult.nextUpdate())
                .concatWith(multiResult.nextUpdate())

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), valueArray.length, "stateList size");

        for (int i = 0, last = valueArray.length - 1; i < valueArray.length; i++) {
            final ResultState state = stateList.get(i);

            assertEquals(state.getResultIndex(), i);
            assertEquals(state.getAffectedRows(), 1L, "getAffectedRows");
            assertEquals(state.getInsertId(), 0L, "insert id");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasReturningColumn(), "returning column");
        }

    }


    /**
     * @see SimpleQueryTask#bindableAsMulti(BatchBindStmt, TaskAdjutant)
     */
    @Test
    public void bindableAsMultiForQuery() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 90;
        // Postgre server 12.6 bug ,RETURNING clause output_name is converted to lower case by server,so 'mytime' not 'myTime'.
        final String sql = "UPDATE my_types as t SET my_time = ?,my_boolean = TRUE WHERE t.id = ? RETURNING t.id AS id,t.my_time AS mytime,t.my_boolean AS myboolean";

        final LocalTime[] valueArray = new LocalTime[]{LocalTime.MIN, LocalTime.MAX, LocalTime.NOON, LocalTime.now()};
        final List<AtomicReference<ResultState>> stateHolderList = new ArrayList<>(valueArray.length);

        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (LocalTime localTime : valueArray) {
            final List<BindValue> valueList = new ArrayList<>(2);
            valueList.add(BindValue.create(0, PgType.TIME, localTime));
            valueList.add(BindValue.create(1, PgType.BIGINT, bindId++));
            groupList.add(Collections.unmodifiableList(valueList));

            stateHolderList.add(new AtomicReference<>(null));
        }

        final MultiResult multiResult;
        multiResult = SimpleQueryTask.bindableAsMulti(PgStmts.bindableBatch(sql, groupList), adjutant);

        int holderIndex = 0;
        final List<ResultRow> rowList;
        rowList = Flux.from(multiResult.nextQuery(stateHolderList.get(holderIndex++)::set))
                .concatWith(multiResult.nextQuery(stateHolderList.get(holderIndex++)::set))
                .concatWith(multiResult.nextQuery(stateHolderList.get(holderIndex++)::set))
                .concatWith(multiResult.nextQuery(stateHolderList.get(holderIndex)::set))

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(rowList, "rowList");
        assertEquals(rowList.size(), valueArray.length, "rowList size");

        for (int i = 0, last = valueArray.length - 1; i < valueArray.length; i++) {
            final ResultRow row = rowList.get(i);
            final List<BindValue> valueList = groupList.get(i);

            assertEquals(row.get("id", Long.class), valueList.get(1).getValue(), "id");
            final LocalTime resultTime = row.getNonNull("mytime", LocalTime.class);
            final LocalTime bindTime = (LocalTime) valueList.get(0).getNonNullValue();

            assertEquals(resultTime.getHour(), bindTime.getHour(), "hour");
            assertEquals(resultTime.getMinute(), bindTime.getMinute(), "minute");
            assertEquals(resultTime.getSecond(), bindTime.getSecond(), "second");
            // my_time precision is 6 ,so below assert
            assertEquals(resultTime.getLong(ChronoField.MICRO_OF_SECOND)
                    , bindTime.getLong(ChronoField.MICRO_OF_SECOND), "micro second");

            assertEquals(row.get("myboolean"), Boolean.TRUE, "myboolean");

            final ResultState state = stateHolderList.get(i).get();
            assertNotNull(state, "state");

            assertEquals(state.getResultIndex(), i);
            assertEquals(state.getAffectedRows(), 1L, "getAffectedRows");
            assertEquals(state.getInsertId(), 0L, "insert id");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            assertFalse(state.hasMoreFetch(), "more fetch");
            assertTrue(state.hasReturningColumn(), "returning column");

        }

    }

    /*################################## blow private method ##################################*/


}
