package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.mysql.type.City;
import io.jdbd.mysql.type.TrueOrFalse;
import io.jdbd.mysql.util.MySQLArrays;
import io.jdbd.mysql.util.MySQLNumbers;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.ResultStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.testng.Assert.*;

/**
 * <p>
 * This class is base of below:
 *     <ul>
 *         <li>{@link ComQueryDataTypeSuiteTests}</li>
 *     </ul>
 * </p>
 */
abstract class AbstractDataTypeSuiteTests extends AbstractTaskSuiteTests {

    private static final Queue<ClientProtocol> PROTOCOL_QUEUE = new LinkedBlockingQueue<>();

    final Logger LOG = LoggerFactory.getLogger(getClass());


    private final long startId;

    AbstractDataTypeSuiteTests(long startId) {
        this.startId = startId;
    }

    abstract Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant);

    abstract Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant);


    /**
     * @see MySQLType#TINYINT
     * @see MySQLType#TINYINT_UNSIGNED
     */
    final void tinyInt() {
        final long id = startId + 1;
        String column = "my_tinyint";
        MySQLType type = MySQLType.TINYINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Byte.MIN_VALUE);
        testType(id, column, type, Byte.MAX_VALUE);

        testType(id, column, type, (short) Byte.MIN_VALUE);
        testType(id, column, type, (short) Byte.MAX_VALUE);
        testType(id, column, type, (int) Byte.MIN_VALUE);
        testType(id, column, type, (int) Byte.MAX_VALUE);

        testType(id, column, type, (long) Byte.MIN_VALUE);
        testType(id, column, type, (long) Byte.MAX_VALUE);
        testType(id, column, type, BigInteger.valueOf(Byte.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Byte.MAX_VALUE));

        testType(id, column, type, BigDecimal.valueOf(Byte.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Byte.MAX_VALUE));
        testType(id, column, type, Byte.toString(Byte.MIN_VALUE));
        testType(id, column, type, Byte.toString(Byte.MAX_VALUE));


        column = "my_tinyint_unsigned";
        type = MySQLType.TINYINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, (short) 0xFF);
        testType(id, column, type, 0xFF);

        testType(id, column, type, (long) 0xFF);
        testType(id, column, type, BigInteger.valueOf(0xFF));
        testType(id, column, type, BigDecimal.valueOf(0xFF));
        testType(id, column, type, Integer.toString(0xFF));
    }

    /**
     * @see MySQLType#SMALLINT
     * @see MySQLType#SMALLINT_UNSIGNED
     */
    final void smallInt() {
        final long id = startId + 2;
        String column;
        MySQLType type;

        column = "my_smallint";
        type = MySQLType.SMALLINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, (int) Short.MIN_VALUE);
        testType(id, column, type, (int) Short.MAX_VALUE);
        testType(id, column, type, (long) Short.MIN_VALUE);
        testType(id, column, type, (long) Short.MAX_VALUE);

        testType(id, column, type, BigInteger.valueOf(Short.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Short.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Short.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Short.MAX_VALUE));

        testType(id, column, type, Short.toString(Short.MIN_VALUE));
        testType(id, column, type, Short.toString(Short.MAX_VALUE));

        column = "my_smallint_unsigned";
        type = MySQLType.SMALLINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, 0xFFFF);
        testType(id, column, type, (long) 0xFFFF);

        testType(id, column, type, (long) 0xFFFF);
        testType(id, column, type, BigInteger.valueOf(0xFFFF));
        testType(id, column, type, BigInteger.valueOf(0xFFFF));
        testType(id, column, type, BigDecimal.valueOf(0xFFFF));

        testType(id, column, type, BigDecimal.valueOf(0xFFFF));
        testType(id, column, type, Integer.toString(0xFFFF));
    }

    /**
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     */
    final void mediumInt() {
        final long id = startId + 3;

        String column;
        MySQLType type;

        final int max = 0x7FFF_FF, min = (-max) - 1;
        column = "my_mediumint";
        type = MySQLType.MEDIUMINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, min);
        testType(id, column, type, max);
        testType(id, column, type, (long) min);
        testType(id, column, type, (long) max);

        testType(id, column, type, BigInteger.valueOf(min));
        testType(id, column, type, BigInteger.valueOf(max));
        testType(id, column, type, BigDecimal.valueOf(min));
        testType(id, column, type, BigDecimal.valueOf(max));

        testType(id, column, type, Integer.toString(min));
        testType(id, column, type, Integer.toString(max));

        column = "my_mediumint_unsigned";
        type = MySQLType.MEDIUMINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, 0xFFFF_FF);

        testType(id, column, type, (long) 0xFFFF_FF);
        testType(id, column, type, BigInteger.valueOf(0xFFFF_FF));
        testType(id, column, type, BigDecimal.valueOf(0xFFFF_FF));
        testType(id, column, type, Integer.toString(0xFFFF_FF));

    }

    /**
     * @see MySQLType#INT
     * @see MySQLType#INT_UNSIGNED
     */
    final void integer() {
        final long id = startId + 4;

        String column;
        MySQLType type;

        column = "my_int";
        type = MySQLType.INT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, Integer.MIN_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);
        testType(id, column, type, (long) Integer.MIN_VALUE);
        testType(id, column, type, (long) Integer.MAX_VALUE);

        testType(id, column, type, BigInteger.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MAX_VALUE));

        testType(id, column, type, Integer.toString(Integer.MIN_VALUE));
        testType(id, column, type, Integer.toString(Integer.MAX_VALUE));

        column = "my_int_unsigned";
        type = MySQLType.INT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, 0xFFFF_FFFFL);
        testType(id, column, type, BigInteger.valueOf(0xFFFF_FFFFL));
        testType(id, column, type, BigDecimal.valueOf(0xFFFF_FFFFL));
        testType(id, column, type, Long.toString(0xFFFF_FFFFL));

    }

    /**
     * @see MySQLType#BIGINT
     * @see MySQLType#BIGINT_UNSIGNED
     */
    final void bigInt() {
        final long id = startId + 5;
        String column;
        MySQLType type;

        column = "my_bigint";
        type = MySQLType.BIGINT;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);

        testType(id, column, type, Integer.MIN_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);
        testType(id, column, type, Long.MIN_VALUE);
        testType(id, column, type, Long.MAX_VALUE);

        testType(id, column, type, BigInteger.valueOf(Long.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Long.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Long.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Long.MAX_VALUE));

        testType(id, column, type, Long.toString(Long.MIN_VALUE));
        testType(id, column, type, Long.toString(Long.MAX_VALUE));


        column = "my_bigint_unsigned";
        type = MySQLType.BIGINT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, Long.MAX_VALUE);
        testType(id, column, type, MySQLNumbers.MAX_UNSIGNED_LONG);

        testType(id, column, type, new BigDecimal(MySQLNumbers.MAX_UNSIGNED_LONG));
        testType(id, column, type, MySQLNumbers.MAX_UNSIGNED_LONG.toString());
    }

    /**
     * @see MySQLType#DECIMAL
     * @see MySQLType#DECIMAL_UNSIGNED
     */
    final void decimal() {
        final long id = startId + 6;
        String column;
        MySQLType type;

        column = "my_decimal";
        type = MySQLType.DECIMAL;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);

        testType(id, column, type, Short.MIN_VALUE);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MIN_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, (long) Integer.MIN_VALUE);
        testType(id, column, type, (long) Integer.MAX_VALUE);
        testType(id, column, type, BigInteger.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigInteger.valueOf(Integer.MAX_VALUE));

        testType(id, column, type, BigDecimal.valueOf(Integer.MIN_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, new BigDecimal("0.00"));
        testType(id, column, type, new BigDecimal("2343.89"));

        testType(id, column, type, new BigDecimal("-2343.89"));
        testType(id, column, type, "-3423432.33");

        column = "my_decimal_unsigned";
        type = MySQLType.DECIMAL_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, (byte) 0);
        testType(id, column, type, Short.MAX_VALUE);
        testType(id, column, type, Integer.MAX_VALUE);

        testType(id, column, type, (long) Integer.MAX_VALUE);
        testType(id, column, type, BigInteger.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, BigDecimal.valueOf(Integer.MAX_VALUE));
        testType(id, column, type, new BigDecimal("0.00"));

        testType(id, column, type, "3423432.33");
    }

    /**
     * @see MySQLType#FLOAT
     * @see MySQLType#FLOAT_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    final void floatType() {
        final long id = startId + 7;
        String column;
        MySQLType type;

        column = "my_float";
        type = MySQLType.FLOAT;

        testType(id, column, type, null);
        testType(id, column, type, 0.0F);
        testType(id, column, type, -1F);
        testType(id, column, type, 1F);

        testType(id, column, type, "1.0E+36");
        testType(id, column, type, "-1.0E+36");

        column = "my_float_unsigned";
        type = MySQLType.FLOAT_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, 0.0F);
        testType(id, column, type, 1F);
        testType(id, column, type, "1.0E+36");

    }

    /**
     * @see MySQLType#DOUBLE
     * @see MySQLType#DOUBLE_UNSIGNED
     */
    @SuppressWarnings("deprecation")
    final void doubleType() {
        final long id = startId + 8;
        String column;
        MySQLType type;

        column = "my_double";
        type = MySQLType.DOUBLE;

        testType(id, column, type, null);
        testType(id, column, type, 0.0D);
        testType(id, column, type, -1D);
        testType(id, column, type, 1D);

        testType(id, column, type, "1.0E+100");
        testType(id, column, type, "-1.0E+100");

        column = "my_double_unsigned";
        type = MySQLType.DOUBLE_UNSIGNED;

        testType(id, column, type, null);
        testType(id, column, type, 0.0D);
        testType(id, column, type, 1D);
        testType(id, column, type, "1.0E+100");

    }

    /**
     * @see MySQLType#CHAR
     */
    final void charType() {
        final long id = startId + 9;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        String column;
        MySQLType type;

        column = "my_char200";
        type = MySQLType.CHAR;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);

    }

    /**
     * @see MySQLType#VARCHAR
     */
    final void varChar() {
        final long id = startId + 10;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        String column;
        MySQLType type;

        column = "my_var_char200";
        type = MySQLType.VARCHAR;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);

    }

    /**
     * @see MySQLType#BINARY
     */
    final void binary() {
        final long id = startId + 11;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        String column;
        MySQLType type;

        column = "my_binary60";
        type = MySQLType.BINARY;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));

    }

    /**
     * @see MySQLType#VARBINARY
     */
    final void varBinary() {
        final long id = startId + 12;
        final String text = "army's name,; \\  \" 'text' '\032' \0 % _ ";
        String column;
        MySQLType type;

        column = "my_var_binary60";
        type = MySQLType.VARBINARY;

        testType(id, column, type, null);
        testType(id, column, type, "");
        testType(id, column, type, text);
        testType(id, column, type, text.getBytes(StandardCharsets.UTF_8));

    }

    /**
     * @see MySQLType#ENUM
     */
    final void enumType() {
        final long id = startId + 13;
        final String column = "my_enum";
        MySQLType type;

        type = MySQLType.ENUM;

        testType(id, column, type, null);
        testType(id, column, type, TrueOrFalse.T);
        testType(id, column, type, TrueOrFalse.F);
        testType(id, column, type, "T");

        testType(id, column, type, "F");

        type = MySQLType.VARCHAR;

        testType(id, column, type, TrueOrFalse.T);
        testType(id, column, type, TrueOrFalse.F);
        testType(id, column, type, "T");
        testType(id, column, type, "F");

        type = MySQLType.CHAR;

        testType(id, column, type, TrueOrFalse.T);
        testType(id, column, type, TrueOrFalse.F);
        testType(id, column, type, "T");
        testType(id, column, type, "F");

    }

    /**
     * @see MySQLType#SET
     */
    final void setType() {
        final long id = startId + 14;
        final String column = "my_set";
        MySQLType type;

        type = MySQLType.SET;

        testType(id, column, type, null);
        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING));
        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING, City.AOMENG, City.SHANGHAI));
        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING.name()));

        testType(id, column, type, MySQLArrays.asUnmodifiableSet(City.BEIJING.name(), City.AOMENG.name(), City.SHANGHAI.name()));
    }

    /**
     * @see MySQLType#TIME
     */
    final void time() {
        final long id = startId + 15;
        String column;
        MySQLType type;

        column = "my_time";
        type = MySQLType.TIME;

//        testType(id, column, type, null);
//        testType(id, column, type, LocalTime.MIDNIGHT);
//        testType(id, column, type, LocalTime.NOON);
        testType(id, column, type, LocalTime.MAX);


    }


    private ResultRow testType(final long id, final String column, final MySQLType type, @Nullable final Object value) {

        String sql;
        sql = String.format("UPDATE mysql_types AS t SET t.%s = ? WHERE t.id = ? ", column);
        final List<BindValue> bindGroup = new ArrayList<>(2);
        bindGroup.add(BindValue.wrap(0, type, value));
        bindGroup.add(BindValue.wrap(1, MySQLType.BIGINT, id));

        final BindStmt updateStmt, queryStmt;
        updateStmt = Stmts.bind(sql, bindGroup);
        sql = String.format("SELECT t.id,t.%s FROM mysql_types AS t WHERE t.id = ?", column);
        queryStmt = Stmts.single(sql, MySQLType.BIGINT, id);

        final ResultRow row;
        row = getClientProtocol()
                .flatMap(protocol -> executeStmt(protocol, updateStmt, queryStmt))
                .block();
        assertNotNull(row, "row");
        if (value == null) {
            assertNull(row.get(column), column);
        } else {
            assertResult(column, type, row, value);
        }
        return row;
    }

    @SuppressWarnings("deprecation")
    private void assertResult(final String column, final MySQLType type, final ResultRow row, final Object nonNull) {
        boolean useDefault = false;
        switch (type) {
            case TIME: {
                LOG.debug("result:{} ,noNull:{}", row.get(column, String.class), nonNull);
            }
            break;
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                if (nonNull instanceof BigDecimal) {
                    assertEquals(row.getNonNull(column, BigDecimal.class).compareTo((BigDecimal) nonNull), 0, column);
                } else if (nonNull instanceof String) {
                    final BigDecimal r = row.getNonNull(column, BigDecimal.class);
                    assertEquals(r, new BigDecimal((String) nonNull), column);
                } else {
                    useDefault = true;
                }
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                if (nonNull instanceof String) {
                    assertEquals(row.get(column, Float.class), Float.valueOf(Float.parseFloat((String) nonNull)), column);
                } else {
                    useDefault = true;
                }
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                if (nonNull instanceof String) {
                    assertEquals(row.get(column, Double.class), Double.valueOf(Double.parseDouble((String) nonNull)), column);
                } else {
                    useDefault = true;
                }
            }
            break;
            default: {
                // no-op
                useDefault = true;
            }
        }

        if (useDefault) {
            final ResultRowMeta rowMeta = row.getRowMeta();
            assertEquals(rowMeta.getSQLType(column), type, column);
            assertTrue(type.javaType().isInstance(row.getNonNull(column)), column);
            assertEquals(row.get(column, nonNull.getClass()), nonNull, column);
        }

    }


    private Mono<ResultRow> executeStmt(final ClientProtocol protocol, final BindStmt updateStmt
            , final BindStmt queryStmt) {
        final TaskAdjutant adjutant = getTaskAdjutant(protocol);
        return executeUpdate(updateStmt, adjutant)
                .switchIfEmpty(Mono.defer(this::updateFailure))
                .thenMany(executeQuery(queryStmt, adjutant))

                .concatWith(closeProtocol(protocol))
                .onErrorResume(error -> closeProtocol(protocol).then(Mono.error(error)))

                .last();
    }


    private <T> Mono<T> closeProtocol(final ClientProtocol protocol) {
        return protocol.reset()
                .doOnSuccess(v -> PROTOCOL_QUEUE.offer(protocol))
                .then(Mono.empty());
    }


    private <T> Mono<T> updateFailure() {
        return Mono.error(new RuntimeException("update failure"));
    }

    private Mono<ClientProtocol> getClientProtocol() {
        final Mono<ClientProtocol> mono;
        ClientProtocol protocol;
        protocol = PROTOCOL_QUEUE.poll();
        if (protocol == null) {
            mono = ClientProtocolFactory.single(DEFAULT_SESSION_ADJUTANT);
        } else {
            mono = Mono.just(protocol);
        }
        return mono;
    }


}
