package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ComQueryDataTypeSuiteTests extends AbstractDataTypeSuiteTests {

    public ComQueryDataTypeSuiteTests() {
        super(100);
    }

    @Override
    Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.bindUpdate(stmt, adjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.bindQuery(stmt, adjutant);
    }

    /**
     * @see MySQLType#TINYINT
     * @see MySQLType#TINYINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void tinyIntBindAndExtract() {
        tinyInt();
    }

    /**
     * @see MySQLType#SMALLINT
     * @see MySQLType#SMALLINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void smallIntBindAndExtract() {
        smallInt();
    }

    /**
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void mediumIntBindAndExtract() {
        mediumInt();
    }

    /**
     * @see MySQLType#INT
     * @see MySQLType#INT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void intBindAndExtract() {
        integer();
    }

    /**
     * @see MySQLType#BIGINT
     * @see MySQLType#BIGINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT, invocationCount = 100)
    public void bigIntBindAndExtract() {
        bigInt();
    }


}
