package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * <p>
 * This class is test class of {@link SimpleQueryTask}.
 * </p>
 *
 * @see SimpleQueryTask
 */
//@Test(groups = {Group.SIMPLE_QUERY_TASK}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS
//        , Group.SESSION_BUILDER, Group.TASK_TEST_ADVICE, Group.SIMPLE_QUERY_TASK})
public class SimpleQuerySqlTypeSuiteTests extends AbstractStmtTaskTests {


    public SimpleQuerySqlTypeSuiteTests() {
        super(100);
    }

    @Override
    final Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant) {
        return SimpleQueryTask.bindableUpdate(stmt, adjutant);
    }

    @Override
    final Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant) {
        return SimpleQueryTask.bindableQuery(stmt, adjutant);
    }

    /**
     * @see PgType#SMALLINT
     */
    @Test
    public void smallIntBindAndExtract() {
        doSmallIntBindAndExtract();
    }

    /**
     * @see PgType#INTEGER
     */
    @Test
    public void integerBindAndExtract() {
        doIntegerBindAndExtract();
    }

    /**
     * @see PgType#BIGINT
     */
    @Test
    public void bigIntBindAndExtract() {
        doBigintBindAndExtract();
    }

    /**
     * @see PgType#DECIMAL
     */
    @Test
    public void decimalBindAndExtract() {
        doDecimalBindAndExtract();
    }

    /**
     * @see PgType#REAL
     */
    @Test
    public void realBindAndExtract() {
        doRealBindAndExtract();
    }

    /**
     * @see PgType#DOUBLE
     */
    @Test
    public void doubleBindAndExtract() {
        doDoubleBindAndExtract();
    }

    /**
     * @see PgType#BOOLEAN
     */
    @Test
    public void booleanBindAndExtract() {
        doBooleanBindAndExtract();
    }


}
