package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Group;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

/**
 * <p>
 * This class is test class of {@link SimpleQueryTask}.
 * </p>
 *
 * @see SimpleQueryTask
 */
@Test(groups = {Group.SIMPLE_QUERY_TASK}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.TASK_TEST_ADVICE})
public class SimpleQuerySqlTypeSuiteTests extends AbstractStmtTaskTests {


    public SimpleQuerySqlTypeSuiteTests() {
        super(100);
    }

    @Test
    public void int2BindAndExtract() {
        doInt2BindAndExtract();
    }


    @Override
    final BiFunction<BindableStmt, TaskAdjutant, Mono<ResultState>> updateFunction() {
        return SimpleQueryTask::bindableUpdate;
    }

    @Override
    final BiFunction<BindableStmt, TaskAdjutant, Flux<ResultRow>> queryFunction() {
        return SimpleQueryTask::bindableQuery;
    }


}
