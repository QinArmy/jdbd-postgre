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
    public void tinyIntBindAndGet() {
        tinyInt();
    }

    /**
     * @see MySQLType#SMALLINT
     * @see MySQLType#SMALLINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void smallIntBindAndGet() {
        smallInt();
    }

    /**
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void mediumIntBindAndGet() {
        mediumInt();
    }

    /**
     * @see MySQLType#INT
     * @see MySQLType#INT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void intBindAndGet() {
        integer();
    }

    /**
     * @see MySQLType#BIGINT
     * @see MySQLType#BIGINT_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void bigIntBindAndGet() {
        bigInt();
    }

    /**
     * @see MySQLType#DECIMAL
     * @see MySQLType#DECIMAL_UNSIGNED
     */
    @Test(timeOut = TIME_OUT)
    public void decimalBindAndGet() {
        decimal();
    }

    /**
     * @see MySQLType#FLOAT
     * @see MySQLType#FLOAT_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    @Test(timeOut = TIME_OUT)
    public void floatBindAndGet() {
        floatType();
    }

    /**
     * @see MySQLType#DOUBLE
     * @see MySQLType#DOUBLE_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    @Test(timeOut = TIME_OUT)
    public void doubleBindAndGet() {
        doubleType();
    }

    /**
     * @see MySQLType#CHAR
     */
    @Test(timeOut = TIME_OUT)
    public void charBindAndGet() {
        charType();
    }

    /**
     * @see MySQLType#VARCHAR
     */
    @Test(timeOut = TIME_OUT)
    public void varCharBindAndGet() {
        varChar();
    }

    /**
     * @see MySQLType#BINARY
     */
    @Test(timeOut = TIME_OUT)
    public void binaryBindAndGet() {
        binary();
    }

    /**
     * @see MySQLType#VARBINARY
     */
    @Test(timeOut = TIME_OUT)
    public void varBinaryBindAndGet() {
        varBinary();
    }

    /**
     * @see MySQLType#VARBINARY
     */
    @Test
    public void enumBindAndGet() {
        enumType();
    }


    /**
     * @see MySQLType#SET
     */
    @Test
    public void setBindAndGet() {
        setType();
    }

    /**
     * @see MySQLType#TIME
     */
    @Test
    public void timeBindAndGet() {
        time();
    }


}
