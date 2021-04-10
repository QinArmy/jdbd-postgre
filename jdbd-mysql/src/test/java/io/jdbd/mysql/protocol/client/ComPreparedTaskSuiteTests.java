package io.jdbd.mysql.protocol.client;


import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.mysql.Groups;
import io.jdbd.mysql.StmtWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @see ComPreparedTask
 */
@Test(enabled = false, groups = {Groups.COM_STMT_PREPARE}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS, Groups.DATA_PREPARE})
public class ComPreparedTaskSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(ComPreparedTaskSuiteTests.class);

    @Override
    Mono<ResultStates> executeUpdate(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComPreparedTask.update(wrapper, taskAdjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(StmtWrapper wrapper, MySQLTaskAdjutant taskAdjutant) {
        return ComPreparedTask.query(wrapper, taskAdjutant);
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
