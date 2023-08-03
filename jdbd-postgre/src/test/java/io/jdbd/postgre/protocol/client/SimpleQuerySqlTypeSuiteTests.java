package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgNumbers;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.StaticStmt;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Locale;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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
        return SimpleQueryTask.paramUpdate(stmt, adjutant);
    }

    @Override
    final Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant) {
        return SimpleQueryTask.paramQuery(stmt, adjutant);
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

    /**
     * @see PgType#XML
     */
    @Test
    public void xmlBindAndExtract() {
        doXmlBindAndExtract();
    }

    @Test
    public void enumBindAndExtract() {
        doEnumBindAndExtract();
    }

    /**
     * @see PgType#UUID
     */
    @Test
    public void uuidBindAndExtract() {
        doUuidBindAndExtract();
    }

    /**
     * @see PgType#POINT
     */
    @Test
    public void pointBindAndExtract() {
        doPointBindAndExtract();
    }

    /**
     * @see PgType#LINE
     */
    @Test
    public void lineBindAndExtract() {
        doLineBindAndExtract();
    }

    /**
     * @see PgType#LINE_SEGMENT
     */
    @Test
    public void lineSegmentBindAndExtract() {
        doLineSegmentBindAndExtract();
    }

    /**
     * @see PgType#BOX
     */
    @Test
    public void boxBindAndExtract() {
        doBoxBindAndExtract();
    }

    /**
     * @see PgType#PATH
     */
    @Test
    public void pathBindAndExtract() {
        doPathBindAndExtract();
    }

    /**
     * @see PgType#POLYGON
     */
    @Test
    public void polygonBindAndExtract() {
        doPolygonBindAndExtract();
    }

    /**
     * @see PgType#CIRCLES
     */
    @Test
    public void circleBindAndExtract() {
        doCircleBindAndExtract();
    }

    /**
     * @see PgType#CIDR
     */
    @Test
    public void cidrBindAndExtract() {
        doCidrBindAndExtract();
    }

    /**
     * @see PgType#INET
     */
    @Test
    public void inetBindAndExtract() {
        doInetBindAndExtract();
    }

    /**
     * @see PgType#MACADDR
     */
    @Test
    public void macaddrBindAndExtract() {
        doMacaddrBindAndExtract();
    }

    /**
     * @see PgType#MACADDR8
     */
    @Test
    public void macaddr8BindAndExtract() {
        doMacaddr8BindAndExtract();
    }


    /**
     * @see PgType#INT4RANGE
     * @see PgType#INT8RANGE
     * @see PgType#NUMRANGE
     * @see PgType#TSRANGE
     * @see PgType#TSTZRANGE
     * @see PgType#DATERANGE
     */
    @Test
    public void rangeBindAndExtract() {
        doRangeBindAndExtract();
    }


    /**
     * @see PgType#SMALLINT_ARRAY
     */
    @Test
    public void smallintArrayBindAndExtract() {
        doSmallintArrayBindAndExtract();
    }

    /**
     * @see PgType#INTEGER_ARRAY
     */
    @Test
    public void integerArrayBindAndExtract() {
        doIntegerArrayBindAndExtract();
    }


    /**
     * @see PgType#BIGINT_ARRAY
     */
    @Test
    public void bigintArrayBindAndExtract() {
        doBigIntArrayBindAndExtract();
    }

    /**
     * @see PgType#DECIMAL_ARRAY
     */
    @Test
    public void decimalArrayBindAndExtract() {
        doDecimalArrayBindAndExtract();
    }

    /**
     * @see PgType#REAL_ARRAY
     */
    @Test
    public void realArrayBindAndExtract() {
        doRealArrayBindAndExtract();
    }

    /**
     * @see PgType#DOUBLE_ARRAY
     */
    @Test
    public void doubleArrayBindAndExtract() {
        doDoubleArrayBindAndExtract();
    }

    /**
     * @see PgType#BOOLEAN_ARRAY
     */
    @Test
    public void booleanArrayBindAndExtract() {
        doBooleanArrayBindAndExtract();
    }

    /**
     * @see PgType#TIMESTAMP_ARRAY
     */
    @Test
    public void timestampArrayBindAndExtract() {
        doTimestampArrayBindAndExtract();
    }

    /**
     * @see PgType#TIME_ARRAY
     */
    @Test
    public void timeArrayBindAndExtract() {
        doTimeArrayBindAndExtract();
    }

    /**
     * @see PgType#TIMESTAMPTZ_ARRAY
     */
    @Test
    public void timestampTzArrayBindAndExtract() {
        doTimestampTzArrayBindAndExtract();
    }

    /**
     * @see PgType#TIMETZ_ARRAY
     */
    @Test
    public void timeTzArrayBindAndExtract() {
        doTimeTzArrayBindAndExtract();
    }

    /**
     * @see PgType#DATE_ARRAY
     */
    @Test
    public void dateArrayBindAndExtract() {
        doDateArrayBindAndExtract();
    }

    /**
     * @see PgType#DATE_ARRAY
     */
    @Test
    public void bitArrayBindAndExtract() {
        doBitArrayBindAndExtract();
    }

    /**
     * @see PgType#VARBIT_ARRAY
     */
    @Test
    public void varBitArrayBindAndExtract() {
        doVarBitArrayBindAndExtract();
    }

    /**
     * @see PgType#INTERVAL_ARRAY
     */
    @Test
    public void intervalArrayBindAndExtract() {
        doIntervalArrayBindAndExtract();
    }

    /**
     * @see PgType#BYTEA_ARRAY
     */
    @Test
    public void byteaArrayBindAndExtract() {
        doByteaArrayBindAndExtract();
    }


    /**
     * @see PgType#MONEY_ARRAY
     */
    @Test
    public void moneyArrayBindAndExtract() {
        doMoneyArrayBindAndExtract();
    }

    /**
     * @see PgType#VARCHAR_ARRAY
     */
    @Test
    public void varCharArrayBindAndExtract() {
        doVarCharArrayBindAndExtract();
    }

    /**
     * @see PgType#CHAR_ARRAY
     */
    @Test
    public void charArrayBindAndExtract() {
        doCharArrayBindAndExtract();
    }

    /**
     * @see PgType#TEXT_ARRAY
     */
    @Test
    public void textArrayBindAndExtract() {
        doTextArrayBindAndExtract();
    }

    /**
     * @see PgType#JSON_ARRAY
     */
    @Test
    public void jsonArrayBindAndExtract() throws IOException {
        doJsonArrayBindAndExtract();
    }

    /**
     * @see PgType#XML_ARRAY
     */
    @Test
    public void xmlArrayBindAndExtract() {
        doXmlArrayBindAndExtract();
    }

    @Test
    public void genderArrayBindAndExtract() {
        doGenderArrayBindAndExtract();
    }

    /**
     * @see PgType#UUID_ARRAY
     */
    @Test
    public void uuidArrayBindAndExtract() {
        doUuidArrayBindAndExtract();
    }

    /**
     * @see PgType#POINT_ARRAY
     */
    @Test
    public void pointArrayBindAndExtract() {
        doPointArrayBindAndExtract();
    }

    /**
     * @see PgType#LINE_ARRAY
     */
    @Test
    public void lineArrayBindAndExtract() {
        doLineArrayBindAndExtract();
    }

    /**
     * @see PgType#LINE_SEGMENT_ARRAY
     */
    @Test
    public void lineSegmentArrayBindAndExtract() {
        doLineSegmentArrayBindAndExtract();
    }

    /**
     * @see PgType#BOX_ARRAY
     */
    @Test
    public void boxArrayBindAndExtract() {
        doBoxArrayBindAndExtract();
    }

    /**
     * @see PgType#PATH_ARRAY
     */
    @Test
    public void pathArrayBindAndExtract() {
        doPathArrayBindAndExtract();
    }

    /**
     * @see PgType#POLYGON_ARRAY
     */
    @Test
    public void polygonArrayBindAndExtract() {
        doPolygonArrayBindAndExtract();
    }

    /**
     * @see PgType#CIRCLES_ARRAY
     */
    @Test
    public void circlesArrayBindAndExtract() {
        doCirclesArrayBindAndExtract();
    }

    /**
     * @see PgType#TEXT
     * @see PgType#JSON
     */
    @Test
    public void pathParameterBindAndExtract() throws IOException {
        doPathParameterBindAndExtract();
    }


    /**
     * @see io.jdbd.postgre.util.PgNumbers#getMoneyFormat(Locale)
     */
    @Test
    public void validateMoneyFormat() {
        final PgProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);
        Locale currentLocale = null;
        ////SET lc_monetary = 'fr_BE.UTF-8';SELECT '3232333.11'::money as "positive",'0.00'::money as "zero" ,'-6676666533.16'::money as "negative"
        final BigDecimal positive = new BigDecimal("+92233720368547758.07"), negative = new BigDecimal("-92233720368547758.08");
        DecimalFormat format = null;
        try {

            final Locale[] locales = Locale.getAvailableLocales();

            for (Locale locale : locales) {
                if (!PgStrings.hasText(locale.getCountry())) {
                    continue;
                }
                format = PgNumbers.getMoneyFormat(locale);
                if (format == null) {
                    continue;
                }
                currentLocale = locale;
                StaticStmt stmt = PgStmts.stmt(String.format("SELECT '%s'::decimal::money as \"positive\",'0.00'::decimal::money as \"zero\" ,'%s'::decimal::money as \"negative\" ", positive.toPlainString(), negative.toPlainString()));

                ResultRow row = SimpleQueryTask.update(PgStmts.stmt(String.format("SET lc_monetary = '%s.UTF-8'", locale)), adjutant)
                        .thenMany(SimpleQueryTask.query(stmt, adjutant))
                        .last()
                        .block();
                assertNotNull(row);

                assertEquals(row.getNonNull("zero", BigDecimal.class).compareTo(BigDecimal.ZERO), 0, "zero");

                if (format.getMaximumFractionDigits() == 0) {
                    BigDecimal v;
                    v = positive.setScale(0, RoundingMode.DOWN);
                    assertEquals(row.getNonNull("positive", BigDecimal.class).compareTo(v), 0, "positive");
                    v = negative.setScale(0, RoundingMode.DOWN);
                    assertEquals(row.getNonNull("negative", BigDecimal.class).compareTo(v), 0, "negative");
                } else {
                    assertEquals(row.getNonNull("positive", BigDecimal.class), positive, "positive");
                    assertEquals(row.getNonNull("negative", BigDecimal.class), negative, "negative");
                }


            }
        } catch (Throwable e) {
            if (format != null) {
                log.info("locale[{}] ,positive[{}] negative[{}]", currentLocale, format.format(positive), format.format(negative));
            }
            throw e;
        } finally {
            releaseConnection(protocol)
                    .block();
        }

    }


}
