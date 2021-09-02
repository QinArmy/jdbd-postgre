package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.Group;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.*;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.result.*;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.stmt.BatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
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
@Test(groups = {Group.SIMPLE_QUERY_TASK}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER, Group.TASK_TEST_ADVICE})
public class SimpleQueryTaskSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleQueryTaskSuiteTests.class);

    private static final long START_ID = 0;


    /**
     * @see SimpleQueryTask#update(StaticStmt, TaskAdjutant)
     */
    @Test
    public void update() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final long bindId = (START_ID + 1);
        final String sql = "UPDATE my_types as t SET my_boolean = true WHERE t.id = " + bindId;
        ResultStates state;
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
        assertFalse(state.hasColumn(), "hasReturningColumn");

    }

    /**
     * @see SimpleQueryTask#query(StaticStmt, TaskAdjutant)
     */
    @Test
    public void query() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        final long bindId = (START_ID + 10);
        final String sql = "SELECT t.* FROM my_types as t WHERE t.id = " + bindId;

        final AtomicReference<ResultStates> stateHolder = new AtomicReference<>(null);
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

        final ResultStates state = stateHolder.get();

        assertNotNull(state, "ResultState");
        assertEquals(state.getAffectedRows(), 0L, "rows");
        assertEquals(state.getInsertId(), 0L, "insert id");
        assertEquals(state.getResultIndex(), 0, "resultIndex");
        assertFalse(state.hasMoreFetch(), "moreFetch");

        assertFalse(state.hasMoreResult(), "moreResult");
        assertTrue(state.hasColumn(), "hasReturningColumn");
    }

    /**
     * @see SimpleQueryTask#batchUpdate(BatchStmt, TaskAdjutant)
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

        final List<ResultStates> stateList;
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
            ResultStates state = stateList.get(i);

            assertEquals(state.getAffectedRows(), 1L, "rows");
            assertEquals(state.getInsertId(), 0L, "insert id");
            assertEquals(state.getResultIndex(), i, "resultIndex");
            assertFalse(state.hasMoreFetch(), "moreFetch");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "moreResult");
            } else {
                assertTrue(state.hasMoreResult(), "moreResult");
            }
            assertFalse(state.hasColumn(), "hasReturningColumn");
        }


    }


    /**
     * @see SimpleQueryTask#batchAsMulti(BatchStmt, TaskAdjutant)
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

        final MultiResult multiResult = SimpleQueryTask.batchAsMulti(PgStmts.group(sqlList), adjutant);

        final AtomicReference<ResultStates> updateRowsHolder = new AtomicReference<>(null);

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

        final ResultStates state = updateRowsHolder.get();
        assertNotNull(state, "state");
        PgTestUtils.assertUpdateOneAndReturningWithMoreResult(state);
    }

    /**
     * @see SimpleQueryTask#batchAsFlux(BatchStmt, TaskAdjutant)
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
        resultList = SimpleQueryTask.batchAsFlux(PgStmts.group(sqlList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(resultList, "resultList");
        assertEquals(resultList.size(), 6, "resultList size");
        Result result;
        ResultStates state;
        ResultRow row;

        // result 0
        result = resultList.get(0);
        assertEquals(result.getResultIndex(), 0, "result index"); // result index must be 0
        assertTrue(result instanceof ResultStates, "first update statement.");
        state = (ResultStates) result;

        assertFalse(state.hasColumn(), "first update statement has returning column.");
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
        assertTrue(result instanceof ResultStates, "second select statement.");

        state = (ResultStates) result;
        assertTrue(state.hasColumn(), "second select statement has returning column.");
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
        assertTrue(result instanceof ResultStates, "third update returning statement.");

        state = (ResultStates) result;
        assertTrue(state.hasColumn(), "third update returning statement has returning column.");
        assertFalse(state.hasMoreFetch(), "third update returning statement more fetch.");
        assertEquals(state.getAffectedRows(), 1L, "third update returning statement affected rows");

        assertEquals(state.getInsertId(), 0L, "third update returning statement insert id");
        assertTrue(state.hasMoreResult(), "third update returning statement more result.");

        // result 3
        result = resultList.get(5);
        assertEquals(result.getResultIndex(), 3, "result index");// result index must be 3
        assertTrue(result instanceof ResultStates, "fourth update statement.");
        state = (ResultStates) result;

        assertFalse(state.hasColumn(), "fourth update statement has returning column.");
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
     * @see SimpleQueryTask#bindableUpdate(BindStmt, TaskAdjutant)
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

        final ResultStates state;
        state = SimpleQueryTask.bindableUpdate(PgStmts.bind(sql, valueList), adjutant)
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
        assertFalse(state.hasColumn(), "hasReturningColumn");

    }


    /**
     * @see SimpleQueryTask#bindableQuery(BindStmt, TaskAdjutant)
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
        row = SimpleQueryTask.bindableQuery(PgStmts.bind(sql, valueList), adjutant)
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

        final List<ResultStates> stateList;
        stateList = SimpleQueryTask.bindableBatchUpdate(PgStmts.bindableBatch(sql, groupList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))
                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), valueArray.length, "stateList size");

        for (int i = 0, last = valueArray.length - 1; i < valueArray.length; i++) {
            ResultStates state = stateList.get(i);
            assertEquals(state.getResultIndex(), i);
            assertEquals(state.getAffectedRows(), 1L, "getAffectedRows");
            assertEquals(state.getInsertId(), 0L, "insert id");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasColumn(), "returning column");

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

        final List<ResultStates> stateList;

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
            final ResultStates state = stateList.get(i);

            assertEquals(state.getResultIndex(), i);
            assertEquals(state.getAffectedRows(), 1L, "getAffectedRows");
            assertEquals(state.getInsertId(), 0L, "insert id");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasColumn(), "returning column");
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
        final List<AtomicReference<ResultStates>> stateHolderList = new ArrayList<>(valueArray.length);

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

            assertEquals(row.getResultIndex(), i, "resultIndex");
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

            final ResultStates state = stateHolderList.get(i).get();
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
            assertTrue(state.hasColumn(), "returning column");

        }

    }

    /**
     * @see SimpleQueryTask#bindableAsFlux(BatchBindStmt, TaskAdjutant)
     */
    @Test
    public void bindableAsFluxForUpdate() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 100;

        // Postgre server 12.6 bug ,RETURNING clause output_name is converted to lower case by server,so 'mytime' not 'myTime'.
        final String sql = "UPDATE my_types as t SET my_time = ?,my_boolean = TRUE WHERE t.id = ? ";

        final LocalTime[] valueArray = new LocalTime[]{LocalTime.MIN, LocalTime.MAX, LocalTime.NOON, LocalTime.now()};

        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (LocalTime localTime : valueArray) {
            final List<BindValue> valueList = new ArrayList<>(2);
            valueList.add(BindValue.create(0, PgType.TIME, localTime));
            valueList.add(BindValue.create(1, PgType.BIGINT, bindId++));
            groupList.add(Collections.unmodifiableList(valueList));
        }

        final List<ResultStates> stateList;
        stateList = SimpleQueryTask.bindableAsFlux(PgStmts.bindableBatch(sql, groupList), adjutant)
                .map(ResultStates.class::cast)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), valueArray.length, "stateList size");

        for (int i = 0, last = valueArray.length - 1; i < valueArray.length; i++) {
            final ResultStates state = stateList.get(i);

            assertEquals(state.getResultIndex(), i);
            assertEquals(state.getAffectedRows(), 1L, "getAffectedRows");
            assertEquals(state.getInsertId(), 0L, "insert id");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            assertFalse(state.hasMoreFetch(), "more fetch");
            assertFalse(state.hasColumn(), "returning column");
        }

    }

    /**
     * @see SimpleQueryTask#bindableAsFlux(BatchBindStmt, TaskAdjutant)
     */
    @Test
    public void bindableAsFluxForQuery() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 110;
        // Postgre server 12.6 bug ,RETURNING clause output_name is converted to lower case by server,so 'mytime' not 'myTime'.
        final String sql = "UPDATE my_types as t SET my_time = ?,my_boolean = TRUE WHERE t.id = ? RETURNING t.id AS id,t.my_time AS mytime,t.my_boolean AS myboolean";

        final LocalTime[] valueArray = new LocalTime[]{LocalTime.MIN, LocalTime.MAX, LocalTime.NOON, LocalTime.now()};

        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (LocalTime localTime : valueArray) {
            final List<BindValue> valueList = new ArrayList<>(2);
            valueList.add(BindValue.create(0, PgType.TIME, localTime));
            valueList.add(BindValue.create(1, PgType.BIGINT, bindId++));
            groupList.add(Collections.unmodifiableList(valueList));

        }

        final List<Result> resultList;
        resultList = SimpleQueryTask.bindableAsFlux(PgStmts.bindableBatch(sql, groupList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(resultList, "resultList");
        assertEquals(resultList.size(), valueArray.length << 1, "resultList size");

        for (int i = 0, j = 0, last = valueArray.length - 1; i < valueArray.length; i++, j += 2) {

            final ResultRow row = (ResultRow) resultList.get(j);
            final List<BindValue> valueList = groupList.get(i);

            assertEquals(row.getResultIndex(), i, "resultIndex");
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

            final ResultStates state = (ResultStates) resultList.get(j + 1);

            assertEquals(state.getResultIndex(), i);
            assertEquals(state.getAffectedRows(), 1L, "getAffectedRows");
            assertEquals(state.getInsertId(), 0L, "insert id");

            if (i == last) {
                assertFalse(state.hasMoreResult(), "more result");
            } else {
                assertTrue(state.hasMoreResult(), "more result");
            }

            assertFalse(state.hasMoreFetch(), "more fetch");
            assertTrue(state.hasColumn(), "returning column");

        }

    }

    /**
     * @see SimpleQueryTask#multiStmtAsMulti(MultiBindStmt, TaskAdjutant)
     */
    @Test
    public void multiStmtAsMulti() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 120;

        String sql;
        List<BindValue> valueList;
        final List<BindStmt> stmtList = new ArrayList<>(3);
        sql = "SELECT t.* FROM my_types AS t WHERE t.id = ?";
        stmtList.add(PgStmts.bind(sql, Collections.singletonList(BindValue.create(0, PgType.BIGINT, bindId++))));

        sql = "UPDATE my_types AS t SET my_time = ?,my_integer = ? WHERE t.id = ? RETURNING t.id AS id,t.my_time AS mytime,t.my_boolean AS myboolean";
        valueList = new ArrayList<>(3);
        valueList.add(BindValue.create(0, PgType.TIME, LocalTime.now()));
        valueList.add(BindValue.create(1, PgType.INTEGER, Integer.MAX_VALUE));
        valueList.add(BindValue.create(2, PgType.BIGINT, bindId++));
        stmtList.add(PgStmts.bind(sql, valueList));

        sql = "UPDATE my_types AS t SET my_char = ?,my_timestamp=? WHERE t.id =?";
        valueList = new ArrayList<>(3);
        valueList.add(BindValue.create(0, PgType.CHAR, "''''\\ ' \\' ' SET balance = balance + 99999.00''"));
        valueList.add(BindValue.create(1, PgType.TIMESTAMP, LocalDateTime.now()));
        valueList.add(BindValue.create(2, PgType.BIGINT, bindId));
        stmtList.add(PgStmts.bind(sql, valueList));

        final MultiResult multiResult;
        multiResult = SimpleQueryTask.multiStmtAsMulti(PgStmts.multi(stmtList), adjutant);

        final AtomicReference<ResultStates> firstStateHolder = new AtomicReference<>(null);
        final AtomicReference<ResultStates> secondStateHolder = new AtomicReference<>(null);

        Flux.from(multiResult.nextQuery(firstStateHolder::set))
                .switchIfEmpty(PgTestUtils.queryNoResponse())
                .collectList()
                .map(PgTestUtils::mapListToOne)
                .map(PgTestUtils.assertRowIdFunction((Long) stmtList.get(0).getParamGroup().get(0).getNonNullValue()))

                .thenMany(multiResult.nextQuery(secondStateHolder::set))
                .switchIfEmpty(PgTestUtils.queryNoResponse())
                .collectList()
                .map(PgTestUtils::mapListToOne)
                .map(PgTestUtils.assertRowIdFunction((Long) stmtList.get(1).getParamGroup().get(2).getNonNullValue()))

                .then(Mono.from(multiResult.nextUpdate()))
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .map(PgTestUtils::assertUpdateOneWithoutMoreResult)

                .then(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .block();

        final ResultStates firstState = firstStateHolder.get();
        assertNotNull(firstState, "firstState");
        PgTestUtils.assertQueryStateWithMoreResult(firstState);

        final ResultStates secondState = secondStateHolder.get();
        assertNotNull(secondState, "secondState");
        PgTestUtils.assertUpdateOneAndReturningWithMoreResult(secondState);

    }

    /**
     * @see SimpleQueryTask#multiStmtAsFlux(MultiBindStmt, TaskAdjutant)
     */
    @Test
    public void multiStmtAsFlux() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 130;

        String sql;
        List<BindValue> valueList;
        final List<BindStmt> stmtList = new ArrayList<>(3);
        sql = "SELECT t.* FROM my_types AS t WHERE t.id = ?";
        stmtList.add(PgStmts.bind(sql, Collections.singletonList(BindValue.create(0, PgType.BIGINT, bindId++))));

        sql = "UPDATE my_types AS t SET my_time = ?,my_integer = ? WHERE t.id = ? RETURNING t.id AS id,t.my_time AS mytime,t.my_boolean AS myboolean";
        valueList = new ArrayList<>(3);
        valueList.add(BindValue.create(0, PgType.TIME, LocalTime.now()));
        valueList.add(BindValue.create(1, PgType.INTEGER, Integer.MAX_VALUE));
        valueList.add(BindValue.create(2, PgType.BIGINT, bindId++));
        stmtList.add(PgStmts.bind(sql, valueList));

        sql = "UPDATE my_types AS t SET my_varchar = ?,my_timestamp=? WHERE t.id =? RETURNING t.id AS id, t.my_varchar AS myvarchar";
        valueList = new ArrayList<>(3);
        valueList.add(BindValue.create(0, PgType.VARCHAR, "''''\\' \\ ' ' SET balance = balance + 99999.00''"));
        valueList.add(BindValue.create(1, PgType.TIMESTAMP, LocalDateTime.now()));
        valueList.add(BindValue.create(2, PgType.BIGINT, bindId));
        stmtList.add(PgStmts.bind(sql, valueList));

        final List<Result> resultList;
        resultList = SimpleQueryTask.multiStmtAsFlux(PgStmts.multi(stmtList), adjutant)
                .switchIfEmpty(PgTestUtils.queryNoResponse())

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(resultList, "resultList");
        assertEquals(resultList.size(), 6, "resultList size");

        ResultStates state;
        ResultRow row;

        row = (ResultRow) resultList.get(0);
        assertEquals(row.getResultIndex(), 0, "resultIndex");
        assertEquals(row.get("id"), stmtList.get(0).getParamGroup().get(0).getNonNullValue(), "stmt one id");
        state = (ResultStates) resultList.get(1);
        assertEquals(state.getResultIndex(), 0, "resultIndex");
        PgTestUtils.assertQueryStateWithMoreResult(state);

        row = (ResultRow) resultList.get(2);
        assertEquals(row.getResultIndex(), 1, "resultIndex");
        assertEquals(row.get("id"), stmtList.get(1).getParamGroup().get(2).getNonNullValue(), "stmt two id");
        state = (ResultStates) resultList.get(3);
        assertEquals(state.getResultIndex(), 1, "resultIndex");
        PgTestUtils.assertUpdateOneAndReturningWithMoreResult(state);


        row = (ResultRow) resultList.get(4);
        assertEquals(row.getResultIndex(), 2, "resultIndex");
        assertEquals(row.get("id"), stmtList.get(2).getParamGroup().get(2).getNonNullValue(), "stmt three id");
        assertEquals(row.get("myvarchar"), stmtList.get(2).getParamGroup().get(0).getNonNullValue(), "stmt three my_varchar");
        state = (ResultStates) resultList.get(5);
        assertEquals(state.getResultIndex(), 2, "resultIndex");
        PgTestUtils.assertUpdateOneAndReturningWithoutMoreResult(state);


    }



    /*################################## blow incorrect user case of all method ##################################*/


    /**
     * @see SimpleQueryTask#update(StaticStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void updateIncorrectUserCase1() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long bindId = START_ID + 140;
        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId
                + ";SELECT t.* FROM my_types as t LIMIT 1;";
        SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("updateIncorrectUserCase1 test failure");

    }


    /**
     * @see SimpleQueryTask#update(StaticStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void updateIncorrectUserCase2() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = "SELECT t.* FROM my_types AS t  WHERE t.id = -9999 LIMIT 1;";
        SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("updateIncorrectUserCase2 test failure");

    }

    /**
     * @see SimpleQueryTask#update(StaticStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void updateIncorrectUserCase3() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = "UPDATE my_type AS SET FROM ";
        SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("updateIncorrectUserCase3 test failure");

    }

    /**
     * @see SimpleQueryTask#query(StaticStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void queryIncorrectUserCase1() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE id = 1 ";

        SimpleQueryTask.query(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("queryIncorrectUserCase1 test failure");
    }

    /**
     * @see SimpleQueryTask#query(StaticStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void queryIncorrectUserCase2() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = "SELECT b.* FROM my_types as t LIMIT ";

        SimpleQueryTask.query(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("queryIncorrectUserCase2 test failure");
    }


    /**
     * @see SimpleQueryTask#query(StaticStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void queryIncorrectUserCase3() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = "SELECT t.* FROM my_types AS t WHERE t.id = 19 ; SELECT t.id,t.my_time AS myTime FROM my_types AS t WHERE t.id = 3 ";

        SimpleQueryTask.query(PgStmts.stmt(sql), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("queryIncorrectUserCase3 test failure");
    }

    /**
     * @see SimpleQueryTask#batchUpdate(BatchStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void batchUpdateInCorrectUserCase1() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 145;

        final List<String> sqlList = new ArrayList<>(4);

        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types AS b SET b.my_boolean = 1 WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types AS t SET my_time '?' = TRUE WHERE t.id = " + bindId);

        SimpleQueryTask.batchUpdate(PgStmts.group(sqlList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("batchUpdateInCorrectUserCase1 test failure");
    }

    /**
     * @see SimpleQueryTask#batchUpdate(BatchStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void batchUpdateInCorrectUserCase2() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 150;

        final List<String> sqlList = new ArrayList<>(4);

        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++);
        sqlList.add("SELECT t.* FROM my_types AS t  WHERE t.id = " + bindId);

        SimpleQueryTask.batchUpdate(PgStmts.group(sqlList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("batchUpdateInCorrectUserCase2 test failure");
    }


    /**
     * @see SimpleQueryTask#batchUpdate(BatchStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void batchUpdateInCorrectUserCase3() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 150;

        final List<String> sqlList = new ArrayList<>(4);

        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++ + ";" + sqlList.get(0));
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId++);
        sqlList.add("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = " + bindId);

        SimpleQueryTask.batchUpdate(PgStmts.group(sqlList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("batchUpdateInCorrectUserCase3 test failure");
    }


    /**
     * @see SimpleQueryTask#bindableUpdate(BindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void bindableUpdateInCorrectUserCase1() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long bindId = START_ID + 155;
        final String sql = "UPDATE my_types AS t SET b.my_time = ? WHERE t.id = ?";
        final List<BindValue> paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, PgType.TIME, LocalTime.now()));
        paramGroup.add(BindValue.create(1, PgType.BIGINT, bindId));

        SimpleQueryTask.bindableUpdate(PgStmts.bind(sql, paramGroup), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("bindableUpdateInCorrectUserCase1 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableUpdate(BindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void bindableUpdateInCorrectUserCase2() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 160;
        final String sql = "UPDATE my_types AS t SET my_time = ? WHERE t.id = ?;UPDATE my_types AS t SET my_boolean = ? WHERE t.id = ?";
        final List<BindValue> paramGroup = new ArrayList<>(4);

        paramGroup.add(BindValue.create(0, PgType.TIME, LocalTime.now()));
        paramGroup.add(BindValue.create(1, PgType.BIGINT, bindId++));
        paramGroup.add(BindValue.create(2, PgType.BOOLEAN, Boolean.TRUE));
        paramGroup.add(BindValue.create(3, PgType.BIGINT, bindId));

        SimpleQueryTask.bindableUpdate(PgStmts.bind(sql, paramGroup), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("bindableUpdateInCorrectUserCase2 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableUpdate(BindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void bindableUpdateInCorrectUserCase3() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long bindId = START_ID + 165;
        final String sql = "SELECT t.id AS id ,t.my_time AS myTime FROM my_types AS t WHERE t.id = ?";
        final List<BindValue> paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, PgType.BIGINT, bindId));

        SimpleQueryTask.bindableUpdate(PgStmts.bind(sql, paramGroup), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .last()
                .block();

        fail("bindableUpdateInCorrectUserCase3 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableQuery(BindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void bindableQueryIncorrectUserCase1() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long bindId = START_ID + 170;

        final String sql = "SELECT t.id AS id ,t.my_time AS myTime FROM my_types AS bb WHERE t.id = ?";
        final List<BindValue> paramGroup = Collections.singletonList(BindValue.create(0, PgType.BIGINT, bindId));

        SimpleQueryTask.bindableQuery(PgStmts.bind(sql, paramGroup), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("bindableQueryIncorrectUserCase1 test failure");
    }

    /**
     * @see SimpleQueryTask#bindableQuery(BindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void bindableQueryIncorrectUserCase2() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long bindId = START_ID + 175;
        final String sql = "UPDATE my_types AS t SET my_zoned_timestamp = ?,my_boolean = ? WHERE t.id = ?";
        final List<BindValue> paramGroup = new ArrayList<>(3);
        paramGroup.add(BindValue.create(0, PgType.TIMESTAMPTZ, OffsetDateTime.now()));
        paramGroup.add(BindValue.create(1, PgType.BOOLEAN, Boolean.TRUE));
        paramGroup.add(BindValue.create(2, PgType.BIGINT, bindId));

        SimpleQueryTask.bindableQuery(PgStmts.bind(sql, paramGroup), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("bindableQueryIncorrectUserCase2 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableQuery(BindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void bindableQueryIncorrectUserCase3() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 180;
        final String sql = "SELECT t.id AS id ,t.my_time AS myTime FROM my_types AS t WHERE t.id = ?;UPDATE my_types AS t SET my_zoned_timestamp = ? WHERE t.id = ?";
        final List<BindValue> paramGroup = new ArrayList<>(3);
        paramGroup.add(BindValue.create(0, PgType.BIGINT, bindId++));
        paramGroup.add(BindValue.create(1, PgType.TIMESTAMPTZ, OffsetDateTime.now()));
        paramGroup.add(BindValue.create(2, PgType.BIGINT, bindId));

        SimpleQueryTask.bindableQuery(PgStmts.bind(sql, paramGroup), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("bindableQueryIncorrectUserCase2 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableBatchUpdate(BatchBindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void bindableBatchUpdateIncorrectUserCase1() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 185;

        final String sql = "UPDATE my_types AS t SET my_integer = ? WHERE bb.id = ?";
        final int[] valueArray = new int[]{0, 999, Integer.MIN_VALUE, Integer.MAX_VALUE};
        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (int j : valueArray) {
            List<BindValue> paramGroup = new ArrayList<>(2);
            paramGroup.add(BindValue.create(0, PgType.INTEGER, j));
            paramGroup.add(BindValue.create(1, PgType.BIGINT, bindId++));

            groupList.add(paramGroup);
        }


        SimpleQueryTask.bindableBatchUpdate(PgStmts.bindableBatch(sql, groupList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("bindableBatchUpdateIncorrectUserCase1 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableBatchUpdate(BatchBindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void bindableBatchUpdateIncorrectUserCase2() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 190;

        final String sql = "UPDATE my_types AS t SET my_integer = ? WHERE t.id = ?;UPDATE my_types AS t SET my_boolean WHERE t.id = " + bindId++;
        final int[] valueArray = new int[]{0, 999, Integer.MIN_VALUE, Integer.MAX_VALUE};
        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (int j : valueArray) {
            List<BindValue> paramGroup = new ArrayList<>(2);
            paramGroup.add(BindValue.create(0, PgType.INTEGER, j));
            paramGroup.add(BindValue.create(1, PgType.BIGINT, bindId++));

            groupList.add(paramGroup);
        }


        SimpleQueryTask.bindableBatchUpdate(PgStmts.bindableBatch(sql, groupList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("bindableBatchUpdateIncorrectUserCase1 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableBatchUpdate(BatchBindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = SubscribeException.class)
    public void bindableBatchUpdateIncorrectUserCase3() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 195;

        final String sql = "SELECT t.id AS id ,t.my_time AS myTime FROM my_types AS t WHERE t.id = ?";
        final long[] valueArray = new long[]{bindId++, bindId++, bindId++, bindId};
        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (long id : valueArray) {
            groupList.add(Collections.singletonList(BindValue.create(0, PgType.BIGINT, id)));
        }

        SimpleQueryTask.bindableBatchUpdate(PgStmts.bindableBatch(sql, groupList), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        fail("bindableBatchUpdateIncorrectUserCase3 test failure");

    }

    /**
     * @see SimpleQueryTask#bindableAsMulti(BatchBindStmt, TaskAdjutant)
     */
    @Test(expectedExceptions = JdbdSQLException.class)
    public void bindableAsMultiIncorrectUserCase1() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        long bindId = START_ID + 200;

        final String sql = "SELECT t.id AS id ,t.my_time AS myTime ?";
        final long[] valueArray = new long[]{bindId++, bindId++, bindId++, bindId};
        final List<List<BindValue>> groupList = new ArrayList<>(valueArray.length);

        for (long id : valueArray) {
            groupList.add(Collections.singletonList(BindValue.create(0, PgType.BIGINT, id)));
        }

        final MultiResult multiResult;
        multiResult = SimpleQueryTask.bindableAsMulti(PgStmts.bindableBatch(sql, groupList), adjutant);
        Flux.from(multiResult.nextUpdate())

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();


        fail("bindableAsMultiIncorrectUserCase1 test failure");

    }





    /*################################## blow private method ##################################*/


}
