package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.SQLType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is base class of :
 * <u>
 * <li>{@link SimpleQuerySqlTypeSuiteTests}</li>
 * <li>{@link ExtendedQueryTaskSqlTypeSuiteTests}</li>
 * </u>
 * </p>
 */
abstract class AbstractStmtTaskTests extends AbstractTaskTests {

    private final int startId;

    AbstractStmtTaskTests(int startId) {
        this.startId = startId;
    }

    abstract Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant);

    abstract Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant);


    /**
     * @see PgType#SMALLINT
     */
    final void doSmallIntBindAndExtract() {
        final String columnName = "my_smallint";
        final long id = startId + 1;

        testType(columnName, PgType.SMALLINT, null, id);

        testType(columnName, PgType.SMALLINT, Short.MAX_VALUE, id);
        testType(columnName, PgType.SMALLINT, Short.MIN_VALUE, id);
        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.SMALLINT, (byte) 0, id);

        testType(columnName, PgType.SMALLINT, 0, id);
        testType(columnName, PgType.SMALLINT, 0L, id);
        testType(columnName, PgType.SMALLINT, BigDecimal.valueOf(Short.MAX_VALUE), id);
        testType(columnName, PgType.SMALLINT, BigInteger.valueOf(Short.MIN_VALUE), id);

        testType(columnName, PgType.SMALLINT, Short.toString(Short.MAX_VALUE), id);
        testType(columnName, PgType.SMALLINT, Boolean.TRUE, id);
        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);

        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Short.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Short.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Short.toString(Short.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Short.toString(Short.MIN_VALUE), id);

    }

    /**
     * @see PgType#INTEGER
     */
    final void doIntegerBindAndExtract() {
        final String columnName = "my_integer";
        final long id = startId + 2;

        testType(columnName, PgType.INTEGER, null, id);

        testType(columnName, PgType.INTEGER, Integer.MAX_VALUE, id);
        testType(columnName, PgType.INTEGER, Integer.MIN_VALUE, id);
        testType(columnName, PgType.INTEGER, (short) 0, id);
        testType(columnName, PgType.INTEGER, (byte) 0, id);

        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.INTEGER, 0L, id);
        testType(columnName, PgType.INTEGER, BigDecimal.valueOf(Integer.MAX_VALUE), id);
        testType(columnName, PgType.INTEGER, BigInteger.valueOf(Integer.MIN_VALUE), id);

        testType(columnName, PgType.INTEGER, Integer.toString(Integer.MAX_VALUE), id);
        testType(columnName, PgType.INTEGER, Boolean.TRUE, id);
        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);

        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Integer.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Integer.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Integer.toString(Integer.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Integer.toString(Integer.MIN_VALUE), id);
    }


    /**
     * @see PgType#BIGINT
     */
    final void doBigintBindAndExtract() {
        final String columnName = "my_bigint";
        final long id = startId + 3;

        testType(columnName, PgType.BIGINT, null, id);

        testType(columnName, PgType.BIGINT, Long.MAX_VALUE, id);
        testType(columnName, PgType.BIGINT, Long.MIN_VALUE, id);
        testType(columnName, PgType.BIGINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, (byte) 0, id);

        testType(columnName, PgType.BIGINT, 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);
        testType(columnName, PgType.BIGINT, BigDecimal.valueOf(Long.MAX_VALUE), id);
        testType(columnName, PgType.BIGINT, BigInteger.valueOf(Long.MIN_VALUE), id);

        testType(columnName, PgType.BIGINT, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.BIGINT, Boolean.TRUE, id);

        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);
        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Long.MAX_VALUE), id);

        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Long.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MIN_VALUE), id);
    }

    /**
     * @see PgType#DECIMAL
     */
    final void doDecimalBindAndExtract() {
        final String columnName = "my_decimal";
        final long id = startId + 4;

        testType(columnName, PgType.DECIMAL, null, id);

        testType(columnName, PgType.DECIMAL, Long.MAX_VALUE, id);
        testType(columnName, PgType.DECIMAL, Long.MIN_VALUE, id);
        testType(columnName, PgType.DECIMAL, (short) 0, id);
        testType(columnName, PgType.DECIMAL, (byte) 0, id);

        testType(columnName, PgType.DECIMAL, 0, id);
        testType(columnName, PgType.DECIMAL, 0L, id);
        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Long.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Long.MIN_VALUE), id);

        testType(columnName, PgType.DECIMAL, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.DECIMAL, Boolean.TRUE, id);

        testType(columnName, PgType.SMALLINT, (short) 0, id);
        testType(columnName, PgType.BIGINT, 0L, id);
        testType(columnName, PgType.INTEGER, 0, id);
        testType(columnName, PgType.DECIMAL, BigDecimal.valueOf(Long.MAX_VALUE), id);

        testType(columnName, PgType.DECIMAL, BigInteger.valueOf(Long.MIN_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Long.toString(Long.MIN_VALUE), id);
    }

    /**
     * @see PgType#REAL
     */
    final void doRealBindAndExtract() {
        final String columnName = "my_real";
        final long id = startId + 5;

        testType(columnName, PgType.REAL, null, id);

        testType(columnName, PgType.REAL, Float.MAX_VALUE, id);
        testType(columnName, PgType.REAL, Float.MIN_VALUE, id);
        testType(columnName, PgType.REAL, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.REAL, Float.toString(Float.MAX_VALUE), id);

        testType(columnName, PgType.REAL, 0.0F, id);
        testType(columnName, PgType.REAL, Boolean.TRUE, id);
        testType(columnName, PgType.REAL, Boolean.FALSE, id);

        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);

    }

    /**
     * @see PgType#DOUBLE
     */
    final void doDoubleBindAndExtract() {
        final String columnName = "my_double";
        final long id = startId + 6;

        testType(columnName, PgType.DOUBLE, null, id);

        testType(columnName, PgType.DOUBLE, Double.MAX_VALUE, id);
        testType(columnName, PgType.DOUBLE, Double.MIN_VALUE, id);
        testType(columnName, PgType.DOUBLE, Double.toString(Double.MAX_VALUE), id);
        testType(columnName, PgType.DOUBLE, Double.toString(Double.MAX_VALUE), id);

        testType(columnName, PgType.DOUBLE, 0.0D, id);
        testType(columnName, PgType.DOUBLE, Boolean.TRUE, id);
        testType(columnName, PgType.DOUBLE, Boolean.FALSE, id);

        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Float.toString(Float.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Double.toString(Double.MAX_VALUE), id);
        testType(columnName, PgType.VARCHAR, Double.toString(Double.MAX_VALUE), id);

    }

    /**
     * @see PgType#BOOLEAN
     */
    final void doBooleanBindAndExtract() {
        final String columnName = "my_boolean";
        final long id = startId + 7;

        testType(columnName, PgType.BOOLEAN, null, id);

        testType(columnName, PgType.BOOLEAN, true, id);
        testType(columnName, PgType.BOOLEAN, false, id);
        testType(columnName, PgType.BOOLEAN, (byte) 0, id);
        testType(columnName, PgType.BOOLEAN, (byte) 1, id);

        testType(columnName, PgType.BOOLEAN, (short) 0, id);
        testType(columnName, PgType.BOOLEAN, (short) 1, id);
        testType(columnName, PgType.BOOLEAN, 0, id);
        testType(columnName, PgType.BOOLEAN, 1, id);

        testType(columnName, PgType.BOOLEAN, 0L, id);
        testType(columnName, PgType.BOOLEAN, 1L, id);
        testType(columnName, PgType.BOOLEAN, 0.0F, id);
        testType(columnName, PgType.BOOLEAN, 1.0F, id);

        testType(columnName, PgType.BOOLEAN, 0.0D, id);
        testType(columnName, PgType.BOOLEAN, 1.0D, id);
        testType(columnName, PgType.BOOLEAN, BigInteger.ZERO, id);
        testType(columnName, PgType.BOOLEAN, BigInteger.ONE, id);

        testType(columnName, PgType.BOOLEAN, BigDecimal.ZERO, id);
        testType(columnName, PgType.BOOLEAN, BigDecimal.ONE, id);
        testType(columnName, PgType.BOOLEAN, PgConstant.TRUE, id);
        testType(columnName, PgType.BOOLEAN, PgConstant.FALSE, id);

        testType(columnName, PgType.BOOLEAN, "T", id);
        testType(columnName, PgType.BOOLEAN, "F", id);
        testType(columnName, PgType.VARCHAR, PgConstant.TRUE, id);
        testType(columnName, PgType.VARCHAR, PgConstant.FALSE, id);

        testType(columnName, PgType.VARCHAR, "T", id);
        testType(columnName, PgType.VARCHAR, "F", id);

    }


    private void testType(final String columnName, final PgType columnType
            , final @Nullable Object value, final long id) {
        assertNotEquals(columnName, "id", "can't be id");

        //1. update column
        final String updateSql = String.format("UPDATE my_types AS t SET %s = ? WHERE t.id = ?", columnName);

        final List<BindValue> paramGroup = new ArrayList<>(2);
        paramGroup.add(BindValue.wrap(0, columnType, value));
        paramGroup.add(BindValue.wrap(1, PgType.BIGINT, id));

        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final ResultStates states;
        states = executeUpdate(PgStmts.bind(updateSql, paramGroup), adjutant)

                .onErrorResume(releaseConnectionOnError(protocol))
                .block();

        assertNotNull(states, "states");
        PgTestUtils.assertUpdateOneWithoutMoreResult(states, 0);

        // 2. query column
        final String querySql = String.format("SELECT t.id AS \"id\", t.%s \"%s\" FROM my_types AS t WHERE t.id = ?", columnName, columnName);

        final ResultRow row;
        row = executeQuery(PgStmts.bind(querySql, BindValue.wrap(0, PgType.BIGINT, id)), adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .map(this::mapToSingleton)
                .block();

        // 3. assert update result

        assertNotNull(row, "row");
        assertEquals(row.get("id"), id, "id");

        final SQLType sqlType = row.getRowMeta().getSQLType(columnName);
        if (value == null) {
            assertNull(row.get(columnName), columnName);
        } else if (sqlType.isDecimal() && value instanceof String) {
            final BigDecimal v = new BigDecimal((String) value);
            final BigDecimal result = row.getNonNull(columnName, BigDecimal.class);
            assertEquals(result.compareTo(v), 0, columnName);
        } else if (sqlType.javaType() == Double.class && value instanceof String) {
            final double v = Double.parseDouble((String) value);
            assertEquals(row.get(columnName, Double.class), Double.valueOf(v), columnName);
        } else if (sqlType.javaType() == Float.class && value instanceof String) {
            final float v = Float.parseFloat((String) value);
            assertEquals(row.get(columnName, Float.class), Float.valueOf(v), columnName);
        } else if (value instanceof BigDecimal) {
            final BigDecimal v = (BigDecimal) value;
            final BigDecimal result = row.getNonNull(columnName, BigDecimal.class);
            assertEquals(result.compareTo(v), 0, columnName);
        } else if (sqlType.javaType() == Boolean.class && value instanceof String) {
            final String v = (String) value;
            final boolean bindValue;
            if (v.equalsIgnoreCase(PgConstant.TRUE)
                    || v.equalsIgnoreCase("T")) {
                bindValue = true;
            } else if (v.equalsIgnoreCase(PgConstant.FALSE)
                    || v.equalsIgnoreCase("F")) {
                bindValue = false;
            } else {
                throw new IllegalArgumentException(String.format("value[%s] error.", value));
            }
            assertEquals(row.get(columnName, Boolean.class), Boolean.valueOf(bindValue), columnName);
        } else {
            assertEquals(row.get(columnName, value.getClass()), value, columnName);
        }

    }


    private <T> T mapToSingleton(List<T> singletonList) {
        assertEquals(singletonList.size(), 1, "list size");
        return singletonList.get(0);
    }


}
