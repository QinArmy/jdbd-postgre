package io.jdbd.mysql.protocol.client;


import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.mysql.stmt.StmtWrappers;
import io.jdbd.mysql.util.MySQLCodes;
import io.jdbd.mysql.util.MySQLStates;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.stmt.ResultType;
import io.jdbd.stmt.SubscribeException;
import io.jdbd.vendor.JdbdCompositeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.testng.Assert.*;


/**
 * @see ComQueryTask
 */
@Test(groups = {Groups.COM_QUERY}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS
        , Groups.COM_QUERY_WRITER, Groups.DATA_PREPARE})
public class ComQueryTaskSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskSuiteTests.class);

    public ComQueryTaskSuiteTests() {
        super(SubType.COM_QUERY);
    }

    @Override
    Mono<ResultStatus> executeUpdate(BindableStmt wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComQueryTask.bindableUpdate(wrapper, taskAdjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(BindableStmt wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComQueryTask.bindableQuery(wrapper, taskAdjutant);
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    /**
     * @see ComQueryTask#update(String, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void update() {
        LOG.info("update test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String newName = "simonyi4";
        String sql = "UPDATE mysql_types as u SET u.name = '%s' WHERE u.id = 1";
        ResultStatus resultStatus = ComQueryTask.update(String.format(sql, newName), adjutant)
                .block();

        assertNotNull(resultStatus, "resultStates");
        assertEquals(resultStatus.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStatus.getInsertId(), 0L, "insertedId");
        assertEquals(resultStatus.getWarnings(), 0, "warnings");

        assertFalse(resultStatus.hasMoreResults(), "hasMoreResult");


        releaseConnection(adjutant);
        LOG.info("update test success");

    }


    /**
     * @see ComQueryTask#query(String, Consumer, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void query() {
        LOG.info("query test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        String sql;
        AtomicReference<ResultStatus> resultStatesHolder = new AtomicReference<>(null);

        sql = "SELECT t.id,t.name,t.create_time as createTime FROM mysql_types as t ORDER BY t.id LIMIT 50";
        List<ResultRow> resultRowList = ComQueryTask.query(sql, resultStatesHolder::set, adjutant)
                .collectList()
                .block();

        final ResultStatus resultStatus = resultStatesHolder.get();

        assertNotNull(resultStatus, "resultStates");

        assertEquals(resultStatus.getAffectedRows(), 0L, "getAffectedRows");
        assertEquals(resultStatus.getWarnings(), 0, "getWarnings");
        assertEquals(resultStatus.getInsertId(), 0L, "getInsertId");
        assertFalse(resultStatus.hasMoreResults(), "hasMoreResults");


        assertNotNull(resultRowList, "resultRowList");
        assertEquals(resultRowList.size(), 50, "resultRowList size");
        for (ResultRow row : resultRowList) {
            assertNotNull(row.getNonNull("id"));
            assertNotNull(row.getNonNull("name"));
            assertNotNull(row.getNonNull("createTime"));
        }
        releaseConnection(adjutant);
        LOG.info("query test success");
    }

    /**
     * @see ComQueryTask#update(String, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT, dependsOnMethods = {"update"})
    public void delete() {
        LOG.info("delete test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        String sql = "DELETE FROM mysql_types WHERE mysql_types.id = 1";

        ResultStatus resultStatus = ComQueryTask.update(sql, adjutant)
                .block();

        assertNotNull(resultStatus, "resultStates");
        assertEquals(resultStatus.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStatus.getInsertId(), 0L, "inserted");
        assertEquals(resultStatus.getWarnings(), 0, "warnings");

        assertFalse(resultStatus.hasMoreResults(), "hasMoreResults");

        sql = "SELECT u.id,u.name FROM mysql_types as u WHERE u.id = 1";

        List<ResultRow> resultRowList = ComQueryTask.query(sql, MultiResult.EMPTY_CONSUMER, adjutant)
                .collectList()
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertTrue(resultRowList.isEmpty(), "resultRowList is empty");

        LOG.info("delete test success");
        releaseConnection(adjutant);
    }

    /**
     * @see ComQueryTask#update(String, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void updateIsQuery() {
        LOG.info("updateIsQuery test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String sql = "SELECT t.id,t.name,t.create_time as createTime FROM mysql_types as t WHERE t.id > 50 ORDER BY t.id LIMIT 50";
        try {
            ComQueryTask.update(sql, adjutant)
                    .block();
            fail("updateIsQuery test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            fail("updateIsQuery test failure.", e);
        }
        LOG.info("updateIsQuery test success");
        releaseConnection(adjutant);
    }

    /**
     * @see ComQueryTask#bindableUpdate(BindableStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableUpdateIsQuery() {
        LOG.info("bindableUpdateIsQuery test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String sql = "SELECT t.id,t.name,t.create_time as createTime FROM mysql_types as t WHERE t.id > ? ORDER BY t.id LIMIT 50";
        try {
            ComQueryTask.bindableUpdate(StmtWrappers.single(sql, BindValue.create(0, MySQLType.BIGINT, 50L)), adjutant)
                    .block();
            fail("bindableUpdateIsQuery test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            fail("bindableUpdateIsQuery test failure.", e);
        }
        LOG.info("bindableUpdateIsQuery test success");
        releaseConnection(adjutant);
    }

    /**
     * @see ComQueryTask#query(String, Consumer, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void queryIsUpdate() {
        LOG.info("queryIsUpdate test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        String sql = "UPDATE mysql_types as u SET u.name = 'simonyi4' WHERE u.id = 30";

        try {
            ComQueryTask.query(sql, MultiResult.EMPTY_CONSUMER, adjutant)
                    .map(row -> {
                        fail("queryIsUpdate test failure.");
                        return row;
                    })
                    .then()
                    .block();
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.QUERY, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.UPDATE, "getActualType");
        } catch (Throwable e) {
            fail("queryIsUpdate test failure.", e);
        }
        LOG.info("queryIsUpdate test success");
        releaseConnection(adjutant);
    }

    /**
     * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateWithSingleStmtMode() {
        LOG.info("batchUpdateWithSingleStmtMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        String sql;
        final List<String> sqlList = new ArrayList<>(3);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);

        List<ResultStatus> resultStatusList;
        resultStatusList = ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList, 0), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatusList, "resultStatesList");
        assertEquals(resultStatusList.size(), 3, "resultStatesList");

        for (ResultStatus states : resultStatusList) {
            assertEquals(states.getAffectedRows(), 1L, "getAffectedRows");
        }

        LOG.info("batchUpdateWithSingleStmtMode test success");
        releaseConnection(adjutant);
    }

    /**
     * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateWithTempMultiStmtMode() {
        LOG.info("batchUpdateWithSingleStmtMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        assertFalse(Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability()), "negotiatedCapability");

        String sql;
        final List<String> sqlList = new ArrayList<>(5);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 3' WHERE t.id = 33";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 4' WHERE t.id = 34";
        sqlList.add(sql);

        List<ResultStatus> resultStatusList;

        resultStatusList = ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList, 0), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatusList, "resultStatesList");
        assertEquals(resultStatusList.size(), sqlList.size(), "resultStatesList");

        for (ResultStatus states : resultStatusList) {
            assertEquals(states.getAffectedRows(), 1L, "getAffectedRows");
        }

        LOG.info("batchUpdateWithSingleStmtMode test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use cate.
     * </p>
     *
     * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateContainQueryWithSingleStmtMode() {
        LOG.info("batchUpdateContainQuery test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        String sql;
        List<String> sqlList;
        sqlList = new ArrayList<>(3);

        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "SELECT t.id ,t.name ,t.create_time as createTime FROM mysql_types as t ORDER BY t.id LIMIT 3";
        sqlList.add(sql);

        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList, 0), adjutant)
                    .switchIfEmpty(Mono.defer(() -> Mono.error(new RuntimeException("update results is empty"))))
                    .index()
                    .map(tuple2 -> {
                        if (tuple2.getT1() < 2) {
                            assertEquals(tuple2.getT2().getAffectedRows(), 1L, "getAffectedRows");
                        } else {
                            fail("batchUpdateContainQuery don't recognize query statement.");
                        }
                        return tuple2;
                    })
                    .then()
                    .block();

            fail("batchUpdateContainQueryWithSingleStmtMode test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.BATCH_UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            fail("batchUpdateContainQueryWithSingleStmtMode test failure.", e);
        }

        sqlList = new ArrayList<>(3);

        sql = "SELECT t.id ,t.name ,t.create_time as createTime FROM mysql_types as t ORDER BY t.id LIMIT 3";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList), adjutant)
                    .map(states -> {
                        fail("batchUpdateContainQueryWithSingleStmtMode has update result,test failure.");
                        return states;
                    })
                    .then()
                    .block();

            fail("batchUpdateContainQueryWithSingleStmtMode test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.BATCH_UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            fail("batchUpdateContainQueryWithSingleStmtMode test failure.", e);
        }

        LOG.info("batchUpdateContainQuery test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use cate.
     * </p>
     *
     * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateSyntaxWithSingleStmtMode() {
        LOG.info("batchUpdateSyntaxWithSingleStmtMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        String sql;
        List<String> sqlList;
        sqlList = new ArrayList<>(3);

        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2 WHERE t.id = 32";
        sqlList.add(sql);

        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList, 0), adjutant)
                    .switchIfEmpty(Mono.defer(() -> Mono.error(new RuntimeException("update results is empty"))))
                    .index()
                    .map(tuple2 -> {
                        if (tuple2.getT1() < 2) {
                            assertEquals(tuple2.getT2().getAffectedRows(), 1L, "getAffectedRows");
                        } else {
                            fail("batchUpdateSyntaxWithSingleStmtMode don't recognize query statement.");
                        }
                        return tuple2;
                    })
                    .then()
                    .block();
            fail("batchUpdateSyntaxWithSingleStmtMode test failure.");
        } catch (JdbdSQLException e) {
            assertEquals(e.getVendorCode(), MySQLCodes.ER_SYNTAX_ERROR, "getVendorCode");
            assertEquals(e.getSQLState(), MySQLStates.SYNTAX_ERROR, "getSQLState");
        } catch (Throwable e) {
            fail("batchUpdateSyntaxWithSingleStmtMode test failure.", e);
        }

        sqlList = new ArrayList<>(3);

        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0 WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);
        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList), adjutant)
                    .map(states -> {
                        fail("batchUpdateSyntaxWithSingleStmtMode has update result,test failure.");
                        return states;
                    })
                    .then()
                    .block();

            fail("batchUpdateSyntaxWithSingleStmtMode test failure.");
        } catch (JdbdSQLException e) {
            assertEquals(e.getVendorCode(), MySQLCodes.ER_SYNTAX_ERROR, "getVendorCode");
            assertEquals(e.getSQLState(), MySQLStates.SYNTAX_ERROR, "getSQLState");
        } catch (Throwable e) {
            fail("batchUpdateSyntaxWithSingleStmtMode test failure.", e);
        }
        LOG.info("batchUpdateSyntaxWithSingleStmtMode test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use cate.
     * </p>
     *
     * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateContainQueryWithTempMultiMode() {
        LOG.info("batchUpdateContainQueryWithTempMultiMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        assertFalse(Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability()), "negotiatedCapability");

        String sql;
        List<String> sqlList;
        sqlList = new ArrayList<>(4);

        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);
        sql = "SELECT t.id ,t.name ,t.create_time as createTime FROM mysql_types as t ORDER BY t.id LIMIT 3";
        sqlList.add(sql);

        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList, 0), adjutant)
                    .switchIfEmpty(Mono.defer(() -> Mono.error(new RuntimeException("update results is empty"))))
                    .index()
                    .map(tuple2 -> {
                        if (tuple2.getT1() < 3) {
                            assertEquals(tuple2.getT2().getAffectedRows(), 1L, "getAffectedRows");
                        } else {
                            fail("batchUpdateContainQueryWithTempMultiMode don't recognize query statement.");
                        }
                        return tuple2;
                    })
                    .then()
                    .block();

            fail("batchUpdateContainQueryWithTempMultiMode test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.BATCH_UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            if (e instanceof JdbdCompositeException) {
                for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                    LOG.error("", throwable);
                }
            }
            fail("batchUpdateContainQueryWithTempMultiMode test failure.", e);
        }

        sqlList = new ArrayList<>(4);

        sql = "SELECT t.id ,t.name ,t.create_time as createTime FROM mysql_types as t ORDER BY t.id LIMIT 3";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);
        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList), adjutant)
                    .map(states -> {
                        fail("batchUpdateContainQueryWithTempMultiMode has update result,test failure.");
                        return states;
                    })
                    .then()
                    .block();

            fail("batchUpdateContainQueryWithTempMultiMode test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.BATCH_UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            if (e instanceof JdbdCompositeException) {
                for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                    LOG.error("", throwable);
                }
            }
            fail("batchUpdateContainQueryWithTempMultiMode test failure.", e);
        }

        LOG.info("batchUpdateContainQueryWithTempMultiMode test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use cate.
     * </p>
     *
     * @see ComQueryTask#batchUpdate(List, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void batchUpdateSyntaxWithTempMultiMode() {
        LOG.info("batchUpdateSyntaxWithTempMultiMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        assertFalse(Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability()), "negotiatedCapability");

        String sql;
        List<String> sqlList;
        sqlList = new ArrayList<>(4);

        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 3 WHERE t.id = 33";
        sqlList.add(sql);

        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList), adjutant)
                    .map(states -> {
                        fail("batchUpdateSyntaxWithTempMultiMode don't recognize error statement.");
                        return states;
                    })
                    .then()
                    .block();
            fail("batchUpdateSyntaxWithTempMultiMode test failure.");
        } catch (JdbdSQLException e) {
            assertEquals(e.getVendorCode(), MySQLCodes.ER_SYNTAX_ERROR, "getVendorCode");
            assertEquals(e.getSQLState(), MySQLStates.SYNTAX_ERROR, "getSQLState");
        } catch (Throwable e) {
            if (e instanceof JdbdCompositeException) {
                for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                    LOG.error("", throwable);
                }
            }
            fail("batchUpdateSyntaxWithTempMultiMode test failure.", e);
        }

        sqlList = new ArrayList<>(4);

        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 0 WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.my_long_text = 'batch update 3' WHERE t.id = 33";
        sqlList.add(sql);
        try {
            ComQueryTask.batchUpdate(StmtWrappers.stmts(sqlList), adjutant)
                    .map(states -> {
                        fail("batchUpdateSyntaxWithSingleStmtMode has update result,test failure.");
                        return states;
                    })
                    .then()
                    .block();

            fail("batchUpdateSyntaxWithTempMultiMode test failure.");
        } catch (JdbdSQLException e) {
            assertEquals(e.getVendorCode(), MySQLCodes.ER_SYNTAX_ERROR, "getVendorCode");
            assertEquals(e.getSQLState(), MySQLStates.SYNTAX_ERROR, "getSQLState");
        } catch (Throwable e) {
            fail("batchUpdateSyntaxWithTempMultiMode test failure.", e);
        }
        LOG.info("batchUpdateSyntaxWithTempMultiMode test success");
        releaseConnection(adjutant);
    }


    /**
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableBatchWithSingleStmtMode() {
        LOG.info("bindableBatchWithSingleStmtMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String sql = "UPDATE mysql_types as t SET t.my_long_text = ? WHERE t.id = ?";
        final List<List<BindValue>> groupList = new ArrayList<>(3);
        List<BindValue> paramGroup;

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, MySQLType.LONGTEXT, "bindable batch update 1"));
        paramGroup.add(BindValue.create(1, MySQLType.BIGINT, 34L));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, MySQLType.LONGTEXT, "bindable batch update 2"));
        paramGroup.add(BindValue.create(1, MySQLType.BIGINT, 35L));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, MySQLType.LONGTEXT, "bindable batch update 3"));
        paramGroup.add(BindValue.create(1, MySQLType.BIGINT, 36L));
        groupList.add(paramGroup);

        final List<ResultStatus> resultStatusList;
        resultStatusList = ComQueryTask.bindableBatch(StmtWrappers.batchBind(sql, groupList), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatusList, "resultStatesList");
        assertEquals(resultStatusList.size(), groupList.size(), "resultStatesList");

        for (ResultStatus states : resultStatusList) {
            assertEquals(states.getAffectedRows(), 1L, "getAffectedRows");
        }

        LOG.info("bindableBatchWithSingleStmtMode test success");
        releaseConnection(adjutant);
    }

    /**
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableBatchWithTempMultiMode() {
        LOG.info("bindableBatchWithTempMultiMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        assertFalse(Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability()), "negotiatedCapability");

        final String sql = "UPDATE mysql_types as t SET t.my_long_text = ? WHERE t.id = ?";
        final List<List<BindValue>> groupList = new ArrayList<>(4);
        List<BindValue> paramGroup;

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, MySQLType.LONGTEXT, "bindable batch update 1"));
        paramGroup.add(BindValue.create(1, MySQLType.BIGINT, 34L));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, MySQLType.LONGTEXT, "bindable batch update 2"));
        paramGroup.add(BindValue.create(1, MySQLType.BIGINT, 35L));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, MySQLType.LONGTEXT, "bindable batch update 3"));
        paramGroup.add(BindValue.create(1, MySQLType.BIGINT, 36L));
        groupList.add(paramGroup);

        paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.create(0, MySQLType.LONGTEXT, "bindable batch update 4"));
        paramGroup.add(BindValue.create(1, MySQLType.BIGINT, 37L));
        groupList.add(paramGroup);

        final List<ResultStatus> resultStatusList;
        resultStatusList = ComQueryTask.bindableBatch(StmtWrappers.batchBind(sql, groupList), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatusList, "resultStatesList");
        assertEquals(resultStatusList.size(), groupList.size(), "resultStatesList");

        for (ResultStatus states : resultStatusList) {
            assertEquals(states.getAffectedRows(), 1L, "getAffectedRows");
        }

        LOG.info("bindableBatchWithTempMultiMode test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use case.
     * </p>
     *
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableBatchIsQueryWithSingleStmtMode() {
        LOG.info("bindableBatchIsQueryWithSingleStmtMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String sql = "SELECT t.id,t.name FROM mysql_types as t WHERE t.id > ?  ORDER BY t.id  LIMIT 10";
        final List<List<BindValue>> groupList = new ArrayList<>(3);

        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 50)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 100)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 150)));

        try {
            ComQueryTask.bindableBatch(StmtWrappers.batchBind(sql, groupList), adjutant)
                    .map(states -> {
                        fail("bindableBatchIsQueryWithSingleStmtMode test failure");
                        return states;
                    })
                    .then()
                    .block();

            fail("bindableBatchIsQueryWithSingleStmtMode test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.BATCH_UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            if (e instanceof JdbdCompositeException) {
                for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                    LOG.error("", throwable);
                }
            }
            fail("bindableBatchIsQueryWithSingleStmtMode test failure.", e);
        }
        LOG.info("bindableBatchIsQueryWithSingleStmtMode test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use case.
     * </p>
     *
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableBatchIsQueryWithTempMultiMode() {
        LOG.info("bindableBatchIsQueryWithTempMultiMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        assertFalse(Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability()), "negotiatedCapability");

        final String sql = "SELECT t.id,t.name FROM mysql_types as t WHERE t.id > ?  ORDER BY t.id  LIMIT 10";
        final List<List<BindValue>> groupList = new ArrayList<>(4);

        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 50)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 100)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 150)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 160)));

        try {
            ComQueryTask.bindableBatch(StmtWrappers.batchBind(sql, groupList), adjutant)
                    .map(states -> {
                        fail("bindableBatchIsQueryWithTempMultiMode test failure");
                        return states;
                    })
                    .then()
                    .block();

            fail("bindableBatchIsQueryWithTempMultiMode test failure.");
        } catch (SubscribeException e) {
            assertEquals(e.getSubscribeType(), ResultType.BATCH_UPDATE, "getSubscribeType");
            assertEquals(e.getActualType(), ResultType.QUERY, "getActualType");
        } catch (Throwable e) {
            if (e instanceof JdbdCompositeException) {
                for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                    LOG.error("JdbdCompositeException member", throwable);
                }
            }
            fail("bindableBatchIsQueryWithTempMultiMode test failure.", e);
        }
        LOG.info("bindableBatchIsQueryWithTempMultiMode test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use case.
     * </p>
     *
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableBatchSyntaxWithSingleStmtMode() {
        LOG.info("bindableBatchSyntaxWithSingleStmtMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();

        final String sql = "UPDATE mysql_types as t SET t.my_long_text = 'error string WHERE t.id = ?";
        final List<List<BindValue>> groupList = new ArrayList<>(3);
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 34L)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 35L)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 36L)));

        try {
            ComQueryTask.bindableBatch(StmtWrappers.batchBind(sql, groupList), adjutant)
                    .map(states -> {
                        fail("bindableBatchSyntaxWithSingleStmtMode test failure");
                        return states;
                    })
                    .then()
                    .block();

            fail("bindableBatchSyntaxWithSingleStmtMode test failure.");
        } catch (JdbdSQLException e) {
            assertEquals(e.getVendorCode(), MySQLCodes.ER_SYNTAX_ERROR, "getVendorCode");
            assertEquals(e.getSQLState(), MySQLStates.SYNTAX_ERROR, "getSQLState");
        } catch (Throwable e) {
            if (e instanceof JdbdCompositeException) {
                for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                    LOG.error("JdbdCompositeException member", throwable);
                }
            }
            fail("bindableBatchSyntaxWithSingleStmtMode test failure.", e);
        }
        LOG.info("bindableBatchSyntaxWithSingleStmtMode test success");
        releaseConnection(adjutant);
    }

    /**
     * <p>
     * test error use case.
     * </p>
     *
     * @see ComQueryTask#bindableBatch(BatchBindStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void bindableBatchSyntaxWithTempMultiMode() {
        LOG.info("bindableBatchSyntaxWithTempMultiMode test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        assertFalse(Capabilities.supportMultiStatement(adjutant.obtainNegotiatedCapability()), "negotiatedCapability");

        final String sql = "UPDATE mysql_types as t SET t.my_long_text = 'error string WHERE t.id = ?";
        final List<List<BindValue>> groupList = new ArrayList<>(4);
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 34L)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 35L)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 36L)));
        groupList.add(Collections.singletonList(BindValue.create(0, MySQLType.BIGINT, 37L)));

        try {
            ComQueryTask.bindableBatch(StmtWrappers.batchBind(sql, groupList), adjutant)
                    .map(states -> {
                        fail("bindableBatchSyntaxWithTempMultiMode test failure");
                        return states;
                    })
                    .then()
                    .block();

            fail("bindableBatchSyntaxWithTempMultiMode test failure.");
        } catch (JdbdSQLException e) {
            assertEquals(e.getVendorCode(), MySQLCodes.ER_SYNTAX_ERROR, "getVendorCode");
            assertEquals(e.getSQLState(), MySQLStates.SYNTAX_ERROR, "getSQLState");
        } catch (Throwable e) {
            if (e instanceof JdbdCompositeException) {
                for (Throwable throwable : ((JdbdCompositeException) e).getErrorList()) {
                    LOG.error("JdbdCompositeException member", throwable);
                }
            }
            fail("bindableBatchSyntaxWithTempMultiMode test failure.", e);
        }
        LOG.info("bindableBatchSyntaxWithTempMultiMode test success");
        releaseConnection(adjutant);
    }


    /**
     * @see ComQueryTask#update(String, MySQLTaskAdjutant)
     * @see ComQueryTask#bindableUpdate(BindableStmt, MySQLTaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void localInFile() {
        LOG.info("localInFile test start");
        final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();


        LOG.info("localInFile test success");
        releaseConnection(adjutant);
    }


    @Test(timeOut = TIME_OUT)
    public void bigIntBindAndExtract() {
        doBigIntBindAndExtract(LOG);
    }


    @Test(timeOut = TIME_OUT)
    public void dateBindAndExtract() {
        doDateBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void timeBindAndExtract() {
        doTimeBindAndExtract(LOG);
    }


    @Test(timeOut = TIME_OUT)
    public void datetimeBindAndExtract() {
        doDatetimeBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void stringBindAndExtract() {
        doStringBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void binaryBindAndExtract() {
        doBinaryBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void doBitBindAndExtract() {
        doBitBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void tinyint1BindExtract() {
        doTinyint1BindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void numberBindAndExtract() {
        doNumberBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void enumBindAndExtract() {
        doEnumBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void setTypeBindAndExtract() {
        doSetTypeBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void jsonBindAndExtract() throws Exception {
        doJsonBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void tinyBlobBindAndExtract() {
        doTinyBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void blobBindAndExtract() {
        doBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void mediumBlobBindAndExtract() {
        doMediumBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void longBlobBindAndExtract() {
        doLongBlobBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void tinyTextBindAndExtract() {
        doTinyTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void textBindAndExtract() {
        doTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void mediumTextBindAndExtract() {
        doMediumTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void longTextBindAndExtract() {
        doLongTextBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void geometryBindAndExtract() {
        doGeometryBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void pointBindAndExtract() {
        doPointBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void lineStringBindAndExtract() {
        doLineStringBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void polygonBindAndExtract() {
        doPolygonBindAndExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void multiPointBindExtract() {
        doMultiPointBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void multiLineStringBindExtract() {
        doMultiLineStringBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void multiPolygonBindExtract() {
        doMultiPolygonBindExtract(LOG);
    }

    @Test(timeOut = TIME_OUT)
    public void geometryCollectionBindExtract() {
        doGeometryCollectionBindExtract(LOG);
    }


}
