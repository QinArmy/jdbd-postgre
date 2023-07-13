package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/**
 * @see QueryCommandWriter
 */
@Test(groups = {Groups.COM_QUERY_WRITER}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS})
public class QueryCommandWriterSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(QueryCommandWriterSuiteTests.class);

    public QueryCommandWriterSuiteTests() {
        super(SubType.COM_QUERY);
    }

    @Override
    Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant) {
        throw new UnsupportedOperationException();
    }

    @Override
    Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant) {
        throw new UnsupportedOperationException();
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    /**
     * @see QueryCommandWriter#createStaticSingleCommand(String, Supplier, TaskAdjutant)
     */
    @Test(timeOut = TIME_OUT)
    public void createStaticSingleCommand() {
        LOG.info("createStaticSingleCommand test start");
        //final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        //TODO zoro add test code
        LOG.info("createStaticSingleCommand test success");
        // releaseConnection(adjutant);
    }


}
