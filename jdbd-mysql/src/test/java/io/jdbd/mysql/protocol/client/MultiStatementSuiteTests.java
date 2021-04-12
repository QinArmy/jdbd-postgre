package io.jdbd.mysql.protocol.client;

import io.jdbd.*;
import io.jdbd.mysql.*;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.jdbd.vendor.result.ReactorMultiResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;


/**
 * <p>
 * id range [260,299]
 * </p>
 *
 * @see ComQueryTask#multiStmt(List, MySQLTaskAdjutant)
 * @see ComQueryTask#bindableMultiStmt(List, MySQLTaskAdjutant)
 * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
 * @see ComQueryTask#bindableBatch(BatchWrapper, MySQLTaskAdjutant)
 */
@Test(groups = {Groups.MULTI_STMT}, dependsOnGroups = {Groups.COM_QUERY, Groups.DATA_PREPARE})
public class MultiStatementSuiteTests extends AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(MultiStatementSuiteTests.class);

    static final Queue<MySQLTaskAdjutant> MULTI_STMT_TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

    private static final MySQLSessionAdjutant MULTI_STMT_SESSION_ADJUTANT = createMultiStmtSessionAdjutant();

    private final int ROW_COUNT = 10;

    @AfterClass
    public void afterClass() {
        Flux.fromIterable(MULTI_STMT_TASK_ADJUTANT_QUEUE)
                .flatMap(QuitTask::quit)
                .then()
                .block();

        MULTI_STMT_TASK_ADJUTANT_QUEUE.clear();
    }

    /**
     * @see ComQueryTask#multiStmt(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void multiStatement() throws Throwable {
        LOG.info("multiStatement test start");
        final MySQLTaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();

        String sql;
        List<String> sqlList = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = 260";//[1] update
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = 261", LocalDate.now());//[2] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 260 ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 262", LocalTime.now());// [4] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 260 ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 263", LocalTime.now());//[6] update
        sqlList.add(sql);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        //below defer serially subscribe
        final ReactorMultiResults multiResults1 = ComQueryTask.multiStmt(Collections.unmodifiableList(sqlList), adjutant);
        multiResults1.nextUpdate()//1. immediately subscribe update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .then(multiResults1.nextUpdate())// 2.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 3.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(multiResults1.nextUpdate())// 4.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 5.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "5.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(multiResults1.nextUpdate())// 6.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .block();

        //below immediately serially subscribe
        final ReactorMultiResults multiResults2;
        multiResults2 = ComQueryTask.multiStmt(Collections.unmodifiableList(sqlList), adjutant);

        final AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .subscribe();// 1. immediately subscribe [1] update

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .subscribe();//2. immediately subscribe [2] update


        multiResults2.nextQuery()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.immediately subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//3. immediately subscribe [3] query

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .subscribe();//4. immediately subscribe [4] update

        multiResults2.nextQuery()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.immediately subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//5. immediately subscribe [5] query

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)
                .block();//6. immediately subscribe [6] update

        final Throwable error = errorHolder.get();
        if (error != null) {
            throw error;
        }
        LOG.info("multiStatement test success");
        releaseMultiStmtConnection(adjutant);
    }


    /**
     * @see ComQueryTask#bindableMultiStmt(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableMultiStatement() throws Throwable {
        LOG.info("multiStatement test start");
        final MySQLTaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();

        String sql;
        final List<StmtWrapper> stmtWrapperList = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = ?";//[1] update
        stmtWrapperList.add(StmtWrappers.single(sql, MySQLBindValue.create(0, MySQLType.BIGINT, 264)));

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = ?", LocalDate.now());//[2] update
        stmtWrapperList.add(StmtWrappers.single(sql, MySQLBindValue.create(0, MySQLType.BIGINT, 265)));

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > ? ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        stmtWrapperList.add(StmtWrappers.single(sql, MySQLBindValue.create(0, MySQLType.BIGINT, 260)));

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = ?", LocalTime.now());// [4] update
        stmtWrapperList.add(StmtWrappers.single(sql, MySQLBindValue.create(0, MySQLType.BIGINT, 266)));

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > ? ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        stmtWrapperList.add(StmtWrappers.single(sql, MySQLBindValue.create(0, MySQLType.BIGINT, 260)));

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = ?", LocalTime.now());//[6] update
        stmtWrapperList.add(StmtWrappers.single(sql, MySQLBindValue.create(0, MySQLType.BIGINT, 267)));

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        //below defer serially subscribe
        final ReactorMultiResults multiResults1 = ComQueryTask.bindableMultiStmt(stmtWrapperList, adjutant);
        multiResults1.nextUpdate()//1. immediately subscribe update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .then(multiResults1.nextUpdate())// 2.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 3.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(multiResults1.nextUpdate())// 4.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .thenMany(multiResults1.nextQuery())// 5.defer subscribe  query
                .switchIfEmpty(emptyError())
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })

                .then(multiResults1.nextUpdate())// 6.defer subscribe  update
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)

                .block();

        //below immediately serially subscribe
        final ReactorMultiResults multiResults2;
        multiResults2 = ComQueryTask.bindableMultiStmt(stmtWrapperList, adjutant);

        final AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertUpdateSuccess)
                .subscribe();// 1. immediately subscribe [1] update

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertUpdateSuccess)
                .subscribe();//2. immediately subscribe [2] update


        multiResults2.nextQuery()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//3. immediately subscribe [3] query

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertUpdateSuccess)
                .subscribe();//4. immediately subscribe [4] update

        multiResults2.nextQuery()
                .switchIfEmpty(emptyError())
                .doOnError(errorHolder::set)
                .map(this::assertResultRow)
                .count()
                .flatMap(this::assertQueryRowCount)
                .doOnNext(count -> {
                    assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                    statesHolder.set(null);
                })
                .subscribe();//5. immediately subscribe [5] query

        multiResults2.nextUpdate()
                .switchIfEmpty(emptyError())
                .map(this::assertUpdateSuccess)
                .block();//6. immediately subscribe [6] update

        final Throwable error = errorHolder.get();
        if (error != null) {
            throw error;
        }
        LOG.info("multiStatement test success");
        releaseMultiStmtConnection(adjutant);
    }

    /**
     * @see ComQueryTask#multiStmt(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void multiStmtError() {
        LOG.info("multiStmtError test start");
        final MySQLTaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();
        String sql;
        final List<String> sqlList = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = 268";//[1] update
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = 269", LocalDate.now());//[2] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 270", LocalTime.now());// [4] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.not_exits_column= '%s' WHERE t.id = 271", LocalTime.now());//[6] error update sql
        sqlList.add(sql);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        try {
            //below defer serially subscribe
            final ReactorMultiResults multiResults1 = ComQueryTask.multiStmt(Collections.unmodifiableList(sqlList), adjutant);
            multiResults1.nextUpdate()//1. immediately subscribe update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)

                    .then(multiResults1.nextUpdate())// 2.defer subscribe  update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)

                    .thenMany(multiResults1.nextQuery())// 3.defer subscribe  query
                    .switchIfEmpty(emptyError())
                    .map(this::assertResultRow)
                    .count()
                    .flatMap(this::assertQueryRowCount)
                    .doOnNext(count -> {
                        assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                        statesHolder.set(null);
                    })

                    .then(multiResults1.nextUpdate())// 4.defer subscribe  update
                    .switchIfEmpty(emptyError())
                    .map(this::assertUpdateSuccess)

                    .thenMany(multiResults1.nextQuery())// 5.defer subscribe  query
                    .switchIfEmpty(emptyError())
                    .map(this::assertResultRow)
                    .count()
                    .flatMap(this::assertQueryRowCount)
                    .doOnNext(count -> {
                        assertNotNull(statesHolder.get(), "3.defer subscribe  query states.");
                        statesHolder.set(null);
                    })

                    .then(multiResults1.nextUpdate())// 6.defer subscribe  update
                    .map(states -> {
                        fail("multiStmtError");
                        return states;
                    })
                    .block();

            fail("multiStmtError error.");
        } catch (JdbdSQLException e) {
            LOG.info("multiStmtError test success");
        } catch (Throwable e) {
            fail("multiStmtError error.", e);
        } finally {
            releaseMultiStmtConnection(adjutant);
        }

    }

    /**
     * @see ComQueryTask#multiStmt(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void multiStmtErrorSubscribe() {
        LOG.info("multiStmtErrorSubscribe test start");
        final MySQLTaskAdjutant adjutant = obtainMultiStmtTaskAdjutant();

        String sql;
        final List<String> sqlList = new ArrayList<>(6);

        sql = "UPDATE mysql_types as t SET t.name = 'mysql' WHERE t.id = 268";//[1] update
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_date = '%s' WHERE t.id = 269", LocalDate.now());//[2] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT);// [3] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 270", LocalTime.now());// [4] update
        sqlList.add(sql);

        sql = String.format("SELECT t.id as id ,t.name as name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 270 ORDER BY t.id LIMIT %s", ROW_COUNT); //[5] query
        sqlList.add(sql);

        sql = String.format("UPDATE mysql_types as t SET t.my_time= '%s' WHERE t.id = 271", LocalTime.now());//[6] update
        sqlList.add(sql);

        try {
            final ReactorMultiResults multiResults = ComQueryTask.multiStmt(sqlList, adjutant);
            multiResults.nextUpdate()//[1] update
                    .map(this::assertUpdateSuccess)

                    .then(multiResults.nextUpdate()) //[2] update
                    .map(this::assertUpdateSuccess)

                    .then(multiResults.nextUpdate()) //[3] query ,error subscribe
                    .map(this::assertUpdateSuccess)
                    .block();
            fail("multiStmtErrorSubscribe test failure");
        } catch (ErrorSubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            fail("multiStmtErrorSubscribe test failure", e);
        }

        try {
            final ReactorMultiResults multiResults = ComQueryTask.multiStmt(sqlList, adjutant);
            multiResults.nextUpdate()//[1] update
                    .map(this::assertUpdateSuccess)

                    .thenMany(multiResults.nextQuery()) //[2] update,error subscribe
                    .map(this::assertResultRow)

                    .then()
                    .block();
            fail("multiStmtErrorSubscribe test failure");
        } catch (ErrorSubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.QUERY, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.UPDATE, "getActualType");
        } catch (Throwable e) {
            fail("multiStmtErrorSubscribe test failure", e);
        }

        LOG.info("multiStmtErrorSubscribe test success");
        releaseMultiStmtConnection(adjutant);
    }


    /*################################## blow private method ##################################*/

    private Mono<Void> assertQueryRowCount(Long resultRowCount) {
        return resultRowCount == ROW_COUNT
                ? Mono.empty()
                : Mono.error(new RuntimeException(String.format("Query result row count[%s] error", resultRowCount)));
    }


    private ResultStates assertUpdateSuccess(ResultStates states) {
        Assert.assertEquals(states.getAffectedRows(), 1L, "update rows");
        return states;
    }


    private ResultRow assertResultRow(ResultRow row) {
        LOG.trace("id:{},name:{},createTime:{}",
                row.getNonNull("id", Long.class),
                row.getNonNull("name", String.class),
                row.getNonNull("createTime", LocalDateTime.class)
        );
        return row;
    }

    private <T> Mono<T> emptyError() {
        return Mono.defer(() -> Mono.error(new RuntimeException("Query result set is empty.")));
    }


    protected static MySQLTaskAdjutant obtainMultiStmtTaskAdjutant() {
        MySQLTaskAdjutant taskAdjutant;

        taskAdjutant = MULTI_STMT_TASK_ADJUTANT_QUEUE.poll();
        if (taskAdjutant == null) {

            ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, MULTI_STMT_SESSION_ADJUTANT)
                    .block();
            assertNotNull(protocol, "protocol");

            taskAdjutant = protocol.taskExecutor.getAdjutant();
        }

        return taskAdjutant;
    }

    protected static void releaseMultiStmtConnection(MySQLTaskAdjutant adjutant) {
        MULTI_STMT_TASK_ADJUTANT_QUEUE.add(adjutant);
    }


    private static MySQLSessionAdjutant createMultiStmtSessionAdjutant() {
        Map<String, String> map = new HashMap<>();
        if (ClientTestUtils.existsServerPublicKey()) {
            map.put(PropertyKey.sslMode.getKey(), Enums.SslMode.DISABLED.name());
        }
        ClientTestUtils.appendZoneConfig(map);
        map.put(PropertyKey.allowMultiQueries.getKey(), "true");
        return getSessionAdjutantForSingleHost(map);
    }


}