package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.time.LocalTime;
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
public class SimpleQueryTaskSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleQueryTaskSuiteTests.class);

    /**
     * @see SimpleQueryTask#update(Stmt, TaskAdjutant)
     */
    @Test
    public void update() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = "UPDATE my_types as t SET my_boolean = true WHERE t.id = 1";
        ResultState state;
        state = SimpleQueryTask.update(PgStmts.stmt(sql), adjutant)
                .concatWith(releaseConnection(protocol))
                .next()
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

        final String sql = "SELECT t.* FROM my_types as t WHERE t.id = 1";

        final AtomicReference<ResultState> stateHolder = new AtomicReference<>(null);
        final ResultRow row;
        row = SimpleQueryTask.query(PgStmts.stmt(sql, stateHolder::getAndSet), adjutant)
                .concatWith(releaseConnection(protocol))
                .blockLast();

        assertNotNull(row, "row");

        final ResultRowMeta rowMeta = row.getRowMeta();
        assertEquals(rowMeta.getSQLType("id"), PgType.BIGINT, "id sql type");
        assertEquals(row.get("id"), 1L, "id");
        assertNotNull(row.get("create_time"), "create_time");

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

        sqlList.add("UPDATE my_types as t SET my_boolean = true WHERE t.id = 1");
        sqlList.add("UPDATE my_types as t SET my_boolean = false WHERE t.id = 2");

        final List<ResultState> stateList;
        stateList = SimpleQueryTask.batchUpdate(PgStmts.group(sqlList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(releaseConnection(protocol))
                .collectList()
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
     * @see SimpleQueryTask#bindableUpdate(BindableStmt, TaskAdjutant)
     */
    @Test
    public void bindableUpdate() {
        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final String sql = "UPDATE my_types as t SET my_time0 =? WHERE t.id = ?";
        final List<BindValue> valueList = new ArrayList<>(2);
        valueList.add(BindValue.create(0, PgType.TIME, LocalTime.now()));
        valueList.add(BindValue.create(1, PgType.BIGINT, 1L));

        final ResultState state;
        state = SimpleQueryTask.bindableUpdate(PgStmts.bindable(sql, valueList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(releaseConnection(protocol))
                .blockLast();

        assertNotNull(state, "ResultState");
        assertEquals(state.getAffectedRows(), 1L, "rows");
        assertEquals(state.getInsertId(), 0L, "insert id");
        assertEquals(state.getResultIndex(), 0, "resultIndex");
        assertFalse(state.hasMoreFetch(), "moreFetch");

        assertFalse(state.hasMoreResult(), "moreResult");
        assertFalse(state.hasReturningColumn(), "hasReturningColumn");

    }


}
