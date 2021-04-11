package io.jdbd.mysql.protocol.client;


import io.jdbd.MultiResults;
import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.StmtWrapper;
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
@Test(groups = {Groups.COM_QUERY}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS, Groups.DATA_PREPARE})
public class ComQueryTaskSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryTaskSuiteTests.class);

    public ComQueryTaskSuiteTests() {
        super(SubType.COM_QUERY);
    }

    @Override
    Mono<ResultStates> executeUpdate(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComQueryTask.bindableUpdate(wrapper, taskAdjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComQueryTask.bindableQuery(wrapper, taskAdjutant);
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
        ResultStates resultStates = ComQueryTask.update(String.format(sql, newName), adjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStates.getInsertId(), 0L, "insertedId");
        assertEquals(resultStates.getWarnings(), 0, "warnings");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResult");


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
        AtomicReference<ResultStates> resultStatesHolder = new AtomicReference<>(null);

        sql = "SELECT t.id,t.name,t.create_time as createTime FROM mysql_types as t ORDER BY t.id LIMIT 50";
        List<ResultRow> resultRowList = ComQueryTask.query(sql, resultStatesHolder::set, adjutant)
                .collectList()
                .block();

        final ResultStates resultStates = resultStatesHolder.get();

        assertNotNull(resultStates, "resultStates");

        assertEquals(resultStates.getAffectedRows(), 0L, "getAffectedRows");
        assertEquals(resultStates.getWarnings(), 0, "getWarnings");
        assertEquals(resultStates.getInsertId(), 0L, "getInsertId");
        assertFalse(resultStates.hasMoreResults(), "hasMoreResults");


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

        ResultStates resultStates = ComQueryTask.update(sql, adjutant)
                .block();

        assertNotNull(resultStates, "resultStates");
        assertEquals(resultStates.getAffectedRows(), 1L, "affectedRows");
        assertEquals(resultStates.getInsertId(), 0L, "inserted");
        assertEquals(resultStates.getWarnings(), 0, "warnings");

        assertFalse(resultStates.hasMoreResults(), "hasMoreResults");

        sql = "SELECT u.id,u.name FROM mysql_types as u WHERE u.id = 1";

        List<ResultRow> resultRowList = ComQueryTask.query(sql, MultiResults.EMPTY_CONSUMER, adjutant)
                .collectList()
                .block();

        assertNotNull(resultRowList, "resultRowList");
        assertTrue(resultRowList.isEmpty(), "resultRowList is empty");

        LOG.info("delete test success");
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
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);

        List<ResultStates> resultStatesList;

        resultStatesList = ComQueryTask.batchUpdate(Collections.unmodifiableList(sqlList), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatesList, "resultStatesList");
        assertEquals(resultStatesList.size(), 3, "resultStatesList");

        for (ResultStates states : resultStatesList) {
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

        String sql;
        final List<String> sqlList = new ArrayList<>(5);
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 0' WHERE t.id = 30";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 1' WHERE t.id = 31";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 2' WHERE t.id = 32";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 3' WHERE t.id = 33";
        sqlList.add(sql);
        sql = "UPDATE mysql_types as t SET t.name = 'batch update 4' WHERE t.id = 34";
        sqlList.add(sql);

        List<ResultStates> resultStatesList;

        resultStatesList = ComQueryTask.batchUpdate(Collections.unmodifiableList(sqlList), adjutant)
                .collectList()
                .block();

        assertNotNull(resultStatesList, "resultStatesList");
        assertEquals(resultStatesList.size(), 5, "resultStatesList");

        for (ResultStates states : resultStatesList) {
            assertEquals(states.getAffectedRows(), 1L, "getAffectedRows");
        }

        LOG.info("batchUpdateWithSingleStmtMode test success");
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
