package io.jdbd.postgre.statement;

import io.jdbd.JdbdException;
import io.jdbd.postgre.Group;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.statement.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.testng.Assert.*;

/**
 * This class is test class of {@code io.jdbd.postgre.session.PgPreparedStatement}
 */
@Test(groups = {Group.PREPARED_STMT}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER
        , Group.TASK_TEST_ADVICE, Group.SIMPLE_QUERY_TASK, Group.EXTENDED_QUERY_TASK, Group.STMT_TEST_ADVICE})
public class PreparedStatementSuiteTests extends AbstractStatementTests {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementSuiteTests.class);

    private static final long START_ID = 400L;

    /**
     * @see PreparedStatement#executeUpdate()
     */
    @Test(timeOut = TIME_OUT)
    public void updateWithoutParameter() {
        final LocalDatabaseSession session;
        session = getSession();
        final long id = START_ID + 1;
        final String sql = String.format("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = %s", id);

        final ResultStates states;
        states = Mono.from(session.prepare(sql))
                .flatMapMany(PreparedStatement::executeUpdate)
                .blockFirst();

        assertNotNull(states, "states");
        PgTestUtils.assertUpdateOneWithoutMoreResult(states);

        closeSession(session);
    }

    /**
     * @see PreparedStatement#executeQuery(Consumer)
     */
    @Test(timeOut = TIME_OUT)
    public void queryWithoutParameter() {
        final LocalDatabaseSession session;
        session = getSession();
        final long id = START_ID + 2;
        final String sql = String.format("SELECT t.* FROM my_types AS t WHERE t.id = %s", id);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        final List<ResultRow> rowList;

        rowList = Flux.from(session.prepare(sql))
                .flatMap(PreparedStatement::executeQuery)
                .collectList()
                .block();

        assertNotNull(rowList, "rowList");
        assertFalse(rowList.isEmpty(), "rowList is empty.");

        ResultStates states = statesHolder.get();
        assertNotNull(states, "ResultStates");
        assertEquals(states.rowCount(), 1, "rowCount");

        closeSession(session);
    }

    /**
     * @see PreparedStatement#executeBatchUpdate()
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateWithoutParameter() {
        final LocalDatabaseSession session;
        session = getSession();

        final long id = START_ID + 3;
        final String sql = String.format("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = %s", id);

        final List<ResultStates> statesList;

        statesList = Mono.from(session.prepare(sql))
                .flatMapMany(statement -> {
                    statement.addBatch();
                    return statement.executeBatchUpdate();
                })
                .collectList()
                .block();

        assertNotNull(statesList, "statesList");
        assertEquals(statesList.size(), 1, "statesList size");
        PgTestUtils.assertUpdateOneWithoutMoreResult(statesList.get(0));

        closeSession(session);
    }

    /**
     * @see PreparedStatement#executeBatchAsMulti()
     */
    @Test(timeOut = TIME_OUT)
    public void batchAsMultiWithoutParameter() {
        final LocalDatabaseSession session;
        session = getSession();

        final long id = START_ID + 4;
        final String sql = String.format("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = %s", id);

        final ResultStates states;
        states = Mono.from(session.prepare(sql))
                .flatMap(statement -> {
                    statement.addBatch();
                    return Mono.from(statement.executeBatchAsMulti().nextUpdate());
                })
                .block();

        assertNotNull(states, "states");
        PgTestUtils.assertUpdateOneWithoutMoreResult(states);

        closeSession(session);
    }

    /**
     * @see PreparedStatement#executeBatchAsFlux()
     */
    @Test(timeOut = TIME_OUT)
    public void batchAsFluxWithoutParameter() {
        final LocalDatabaseSession session;
        session = getSession();

        final long id = START_ID + 5;
        final String sql = String.format("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = %s", id);

        final List<ResultStates> statesList;
        statesList = Mono.from(session.prepare(sql))
                .flatMapMany(statement -> {
                    statement.addBatch();
                    return Flux.from(statement.executeBatchAsFlux());
                })
                .cast(ResultStates.class)
                .collectList()
                .block();

        assertNotNull(statesList, "statesList");
        assertEquals(statesList.size(), 1, "statesList size");

        final ResultStates states = statesList.get(0);
        assertEquals(states.getResultNo(), 0, "resultIndex");
        PgTestUtils.assertUpdateOneWithoutMoreResult(statesList.get(0));

        closeSession(session);

    }

    /**
     * @see PreparedStatement#executeUpdate()
     */
    @Test(timeOut = TIME_OUT)
    public void update() {
        final LocalDatabaseSession session;
        session = getSession();
        final long id = START_ID + 6;
        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = ? ";

        final ResultStates states;
        states = Mono.from(session.prepare(sql))
                .flatMap(statement -> {
                    statement.bind(0, id);
                    return Mono.from(statement.executeUpdate());
                })
                .block();

        assertNotNull(states, "states");

        assertEquals(states.getResultNo(), 0, "resultIndex");
        assertFalse(states.hasMoreFetch(), "moreFetch");
        assertFalse(states.hasMoreResult(), "moreResult");
        assertFalse(states.hasColumn(), "hasColumn");

        PgTestUtils.assertUpdateOneWithoutMoreResult(states);

        closeSession(session);
    }

    /**
     * @see PreparedStatement#executeQuery(Consumer)
     */
    @Test(timeOut = TIME_OUT)
    public void query() {
        final LocalDatabaseSession session;
        session = getSession();
        final long id = START_ID + 7;
        final String sql = "SELECT t.* FROM my_types AS t WHERE t.id = ?";

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);
        final List<ResultRow> rowList;

        rowList = Mono.from(session.prepare(sql))
                .flatMapMany(statement -> {
                    statement.bind(0, id);
                    return statement.executeQuery(statesHolder::set);
                })
                .collectList()
                .block();
        assertNotNull(rowList, "statesList");
        assertEquals(rowList.size(), 1, "statesList size");
        assertEquals(rowList.get(0).get("id"), id, "id");

        final ResultStates states = statesHolder.get();
        assertNotNull(states, "states");

        assertEquals(states.getResultNo(), 0, "resultIndex");
        assertFalse(states.hasMoreFetch(), "moreFetch");
        assertFalse(states.hasMoreResult(), "moreResult");
        assertTrue(states.hasColumn(), "hasColumn");

        assertEquals(states.getAffectedRows(), 0L, "affectedRows");
        assertEquals(states.getInsertId(), 0L, "insertId");
        assertEquals(states.rowCount(), 1L, "rowCount");

        closeSession(session);
    }

    /**
     * @see PreparedStatement#executeBatchUpdate()
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdate() {
        final LocalDatabaseSession session;
        session = getSession();
        final int batchCount = 10;
        final long startId = START_ID + 8;

        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = ? ";

        final List<ResultStates> stateList;
        stateList = Mono.from(session.prepare(sql))
                .flatMapMany(statement -> {
                    for (int i = 0; i < batchCount; i++) {
                        statement.bind(0, startId + i);
                        statement.addBatch();
                    }
                    return statement.executeBatchUpdate();
                })
                .collectList()
                .block();

        assertNotNull(stateList, "stateList");
        assertEquals(stateList.size(), batchCount, "stateList size");

        for (int i = 0, last = batchCount - 1; i < batchCount; i++) {
            ResultStates states = stateList.get(i);
            assertEquals(states.getResultNo(), i, "result index");
            if (i == last) {
                PgTestUtils.assertUpdateOneWithoutMoreResult(states);
            } else {
                PgTestUtils.assertUpdateOneWithMoreResult(states);
            }

        }

        closeSession(session);
    }


    /**
     * @see PreparedStatement#executeBatchAsMulti()
     */
    @Test(timeOut = TIME_OUT)
    public void batchAsMulti() {
        final LocalDatabaseSession session;
        session = getSession();

        final int batchCount = 4;
        final long startId = START_ID + 18;

        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = ? ";

        final MultiResult multiResult;
        multiResult = Mono.from(session.prepare(sql))
                .map(statement -> {
                    for (int i = 0; i < batchCount; i++) {
                        statement.bind(0, startId + i);
                        statement.addBatch();
                    }
                    return statement.executeBatchAsMulti();
                }).block();

        assertNotNull(multiResult, "multiResult");

        Mono.from(multiResult.nextUpdate())
                .map(states -> PgTestUtils.assertUpdateOneWithMoreResult(states, 0))

                .then(Mono.from(multiResult.nextUpdate()))
                .map(states -> PgTestUtils.assertUpdateOneWithMoreResult(states, 1))

                .then(Mono.from(multiResult.nextUpdate()))
                .map(states -> PgTestUtils.assertUpdateOneWithMoreResult(states, 2))

                .then(Mono.from(multiResult.nextUpdate()))
                .map(states -> PgTestUtils.assertUpdateOneWithoutMoreResult(states, 3))

                .then()
                .block();

        closeSession(session);
    }

    /**
     * @see PreparedStatement#executeBatchAsFlux()
     */
    @Test(timeOut = TIME_OUT)
    public void batchAsFlux() {
        final LocalDatabaseSession session;
        session = getSession();

        final int batchCount = 10;
        final long startId = START_ID + 22;

        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = ? ";

        final List<ResultStates> statesList;
        statesList = Mono.from(session.prepare(sql))
                .flatMapMany(statement -> {
                    for (int i = 0; i < batchCount; i++) {
                        statement.bind(0, startId + i);
                        statement.addBatch();
                    }
                    return statement.executeBatchAsFlux();
                }).cast(ResultStates.class)
                .collectList()
                .block();

        assertNotNull(statesList, "statesList");
        assertEquals(statesList.size(), batchCount, "statesList size");

        for (int i = 0, last = batchCount - 1; i < batchCount; i++) {
            ResultStates states = statesList.get(i);
            assertEquals(states.getResultNo(), i, "result index");
            if (i == last) {
                PgTestUtils.assertUpdateOneWithoutMoreResult(states);
            } else {
                PgTestUtils.assertUpdateOneWithMoreResult(states);
            }

        }

        closeSession(session);
    }

    /**
     * @see PreparedStatement#abandonBind()
     */
    @Test(timeOut = TIME_OUT)
    public void abandonBind() {
        final LocalDatabaseSession session;
        session = getSession();

        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = ? ";

        final long startId = START_ID + 32;

        final ResultStates states;
        states = Mono.from(session.prepare(sql))
                .flatMap(statement -> Mono.from(statement.abandonBind())) // abandonBind
                .flatMap(s -> {
                    LOG.info("abandonBind success execute next PreparedStatement");
                    return Mono.from(s.prepare(sql));
                })
                .flatMap(statement -> {
                    statement.bind(0, startId);
                    return Mono.from(statement.executeUpdate());
                })
                .block();

        assertNotNull(states, "states");
        PgTestUtils.assertUpdateOneWithoutMoreResult(states, 0);

        closeSession(session);
    }

    /**
     * @see PreparedStatement#bind(int, Object)
     */
    @Test(timeOut = TIME_OUT)
    public void bindingOccurError() {
        final LocalDatabaseSession session;
        session = getSession();

        final String sql = "UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = ? ";

        final long startId = START_ID + 33;

        final ResultStates states;
        states = Mono.from(session.prepare(sql))
                .flatMap(statement -> {// binding occur error start.
                    Mono<DatabaseSession> sessionMono;
                    try {
                        statement.bind(-3, 0);
                        sessionMono = Mono.error(new RuntimeException("bind method error"));
                    } catch (JdbdException e) {
                        sessionMono = Mono.just(statement.getSession());
                    }
                    return sessionMono;
                }) // binding occur error end.
                .flatMap(s -> Mono.from(s.prepare(sql)))
                .flatMap(statement -> {
                    statement.bind(0, startId);
                    return Mono.from(statement.executeUpdate());
                })
                .block();

        assertNotNull(states, "states");
        PgTestUtils.assertUpdateOneWithoutMoreResult(states, 0);

        closeSession(session);
    }


}
