package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;

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


    /**
     * @see PgType#TIMESTAMP
     */
    @Test
    public void timestampBindExtract() {
        doTimestampBindExtract();
    }

    /**
     * @see PgType#TIMESTAMPTZ
     */
    @Test
    public void timestampTzBindExtract() {
        doTimestampTzBindExtract();
    }

    /**
     * @see PgType#DATE
     */
    @Test
    public void dateBindExtract() {
        doDateBindAndExtract();
    }


    /**
     * @see PgType#TIME
     */
    @Test
    public void timeBindExtract() {
        doTimeBindAndExtract();
    }

    /**
     * @see PgType#TIMETZ
     */
    @Test
    public void timeTzBindExtract() {
        doTimeTzBindAndExtract();
    }

    /**
     * @see PgType#BIT
     */
    @Test
    public void bitBindExtract() {
        doBit64BindAndExtract();
        doBit32BindAndExtract();
    }

    /**
     * @see PgType#VARBIT
     */
    @Test
    public void varBitBindExtract() {
        doVarBitBindAndExtract();
    }

    /**
     * @see PgType#INTERVAL
     */
    @Test
    public void intervalBindExtract() {
        doIntervalBindAndExtract();
    }

    /**
     * @see PgType#BYTEA
     */
    @Test
    public void byteaBindExtract() {
        doByteaBindAndExtract();
    }

    /**
     * @see PgType#MONEY
     */
    @Test
    public void moneyBindExtract() {
        doMoneyBindAndExtract();
    }

    /**
     * @see PgType#VARCHAR
     */
    @Test
    public void varcharBindExtract() {
        doVarcharBindAndExtract();
    }

    /**
     * @see PgType#CHAR
     */
    @Test
    public void charBindExtract() {
        doCharBindAndExtract();
    }

    /**
     * @see PgType#TEXT
     */
    @Test
    public void textBindExtract() {
        doTextBindAndExtract();
    }

    /**
     * @see PgType#JSON
     */
    @Test
    public void jsonBindExtract() throws IOException {
        doJsonBindAndExtract();
    }

    /**
     * @see PgType#JSONB
     */
    @Test
    public void jsonbBindExtract() throws IOException {
        doJsonbBindAndExtract();
    }

}
