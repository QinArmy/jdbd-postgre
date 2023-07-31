package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.KeyMode;
import io.jdbd.postgre.ClientTestUtils;
import io.jdbd.postgre.Group;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgStreams;
import io.jdbd.result.FieldType;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.ResultStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

@Test(groups = {Group.TASK_TEST_ADVICE}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER})
public class TaskTestAdvice extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(TaskTestAdvice.class);

    @BeforeClass
    public static void beforeClass() throws IOException {
        List<String> sqlList = new ArrayList<>(6);
        sqlList.add("DROP TABLE IF EXISTS my_types");
        sqlList.add("DROP TYPE IF EXISTS gender");
        sqlList.add("CREATE TYPE gender AS ENUM ('FEMALE','MALE','UNKNOWN')");

        final String sqlDir = ClientTestUtils.getTestResourcesPath().toString();
        sqlList.add(PgStreams.readAsString(Paths.get(sqlDir, "data/sql/my_types.sql")));
        sqlList.add("DROP TABLE IF EXISTS my_copies");
        sqlList.add(PgStreams.readAsString(Paths.get(sqlDir, "data/sql/my_copies.sql")));

        final PgProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long resultCount;
        resultCount = SimpleQueryTask.batchUpdate(PgStmts.batch(sqlList), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(TaskTestAdvice.releaseConnection(protocol))
                .count()
                .block();

        assertEquals(resultCount, sqlList.size(), "resultCount");
    }

    @Test
    public void insertTestData() {
        final String prefix = "INSERT INTO my_types(my_xml,my_xml_array,my_xml_array_2) VALUES";

        final String row = "(XMLPARSE(DOCUMENT '<?xml version=\"1.0\"?><book></book>'), '{\"<book></book>\"}', '{{\"<book></book>\"}}')";
        final int rowCount = 1000;

        StringBuilder builder = new StringBuilder(prefix.length() + row.length() * rowCount + rowCount);
        builder.append(prefix);

        builder.append(row);
        for (int i = 1; i < rowCount; i++) {
            builder.append(",")
                    .append(row);
        }

        final PgProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final ResultStates state;
        state = SimpleQueryTask.update(PgStmts.stmt(builder.toString()), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(TaskTestAdvice.releaseConnection(protocol))
                .blockLast();

        assertNotNull(state, "state");
        assertEquals(state.getAffectedRows(), rowCount, "affectedRows");

    }

    @Test(dependsOnMethods = {"insertTestData"})
    public void rowMetaValidate() {
        final String sql = "SELECT t.* FROM my_types as t LIMIT 10";

        final PgProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        SimpleQueryTask.query(PgStmts.stmt(sql), adjutant)
                .switchIfEmpty(PgTestUtils.queryNoResponse())
                .doOnNext(this::assertRowMeta)
                .then()
                .block();

    }

    /**
     * @see #rowMetaValidate()
     */
    private void assertRowMeta(final ResultRow row) {
        final ResultRowMeta meta = row.getRowMeta();

        for (int i = 0, count = meta.getColumnCount(); i < count; i++) {
            assertUnknownFieldTypeMeta(meta, i);
        }

        int index;

        index = meta.getColumnIndex("id");
        assertNotNull(row.get(index), "id");
        assertEquals(meta.getSQLType(index), PgType.BIGINT, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_smallint");
        assertNotNull(row.get(index), "my_smallint");
        assertEquals(meta.getSQLType(index), PgType.SMALLINT, "sqlType");
        assertEquals(meta.getPrecision(index), 2, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_integer");
        assertNotNull(row.get(index), "my_integer");
        assertEquals(meta.getSQLType(index), PgType.INTEGER, "sqlType");
        assertEquals(meta.getPrecision(index), 4, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_decimal");
        assertNotNull(row.get(index), "my_decimal");
        assertEquals(meta.getSQLType(index), PgType.DECIMAL, "sqlType");
        assertEquals(meta.getPrecision(index), 14, "precision");
        assertEquals(meta.getScale(index), 2, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_real");
        assertNotNull(row.get(index), "my_real");
        assertEquals(meta.getSQLType(index), PgType.REAL, "sqlType");
        assertEquals(meta.getPrecision(index), 4, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_double");
        assertNotNull(row.get(index), "my_double");
        assertEquals(meta.getSQLType(index), PgType.DOUBLE, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_boolean");
        assertNotNull(row.get(index), "my_boolean");
        assertEquals(meta.getSQLType(index), PgType.BOOLEAN, "sqlType");
        assertEquals(meta.getPrecision(index), 1, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_timestamp");
        assertNotNull(row.get(index), "my_timestamp");
        assertEquals(meta.getSQLType(index), PgType.TIMESTAMP, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_timestamp_4");
        assertNotNull(row.get(index), "my_timestamp_4");
        assertEquals(meta.getSQLType(index), PgType.TIMESTAMP, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 4, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_zoned_timestamp");
        assertNotNull(row.get(index), "my_zoned_timestamp");
        assertEquals(meta.getSQLType(index), PgType.TIMESTAMPTZ, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_zoned_timestamp_6");
        assertNotNull(row.get(index), "my_zoned_timestamp_6");
        assertEquals(meta.getSQLType(index), PgType.TIMESTAMPTZ, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_date");
        assertNotNull(row.get(index), "my_date");
        assertEquals(meta.getSQLType(index), PgType.DATE, "sqlType");
        assertEquals(meta.getPrecision(index), 4, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_time");
        assertNotNull(row.get(index), "my_time");
        assertEquals(meta.getSQLType(index), PgType.TIME, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_time_2");
        assertNotNull(row.get(index), "my_time_2");
        assertEquals(meta.getSQLType(index), PgType.TIME, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 2, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_zoned_time");
        assertNotNull(row.get(index), "my_zoned_time");
        assertEquals(meta.getSQLType(index), PgType.TIMETZ, "sqlType");
        assertEquals(meta.getPrecision(index), 12, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_zoned_time_1");
        assertNotNull(row.get(index), "my_zoned_time_1");
        assertEquals(meta.getSQLType(index), PgType.TIMETZ, "sqlType");
        assertEquals(meta.getPrecision(index), 12, "precision");
        assertEquals(meta.getScale(index), 1, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_bit");
        assertNotNull(row.get(index), "my_bit");
        assertEquals(meta.getSQLType(index), PgType.BIT, "sqlType");
        assertEquals(meta.getPrecision(index), 64, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_varbit_64");
        assertNotNull(row.get(index), "my_varbit_64");
        assertEquals(meta.getSQLType(index), PgType.VARBIT, "sqlType");
        assertEquals(meta.getPrecision(index), 64, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_interval");
        assertNotNull(row.get(index), "my_interval");
        assertEquals(meta.getSQLType(index), PgType.INTERVAL, "sqlType");
        assertEquals(meta.getPrecision(index), 16, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_bytea");
        assertNotNull(row.get(index), "my_bytea");
        assertEquals(meta.getSQLType(index), PgType.BYTEA, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_money");
        assertNotNull(row.get(index), "my_money");
        assertEquals(meta.getSQLType(index), PgType.MONEY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_varchar");
        assertNotNull(row.get(index), "my_varchar");
        assertEquals(meta.getSQLType(index), PgType.VARCHAR, "sqlType");
        assertEquals(meta.getPrecision(index), 256, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_char");
        assertNotNull(row.get(index), "my_char");
        assertEquals(meta.getSQLType(index), PgType.CHAR, "sqlType");
        assertEquals(meta.getPrecision(index), 128, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_text");
        assertNotNull(row.get(index), "my_text");
        assertEquals(meta.getSQLType(index), PgType.TEXT, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_json");
        assertNotNull(row.get(index), "my_json");
        assertEquals(meta.getSQLType(index), PgType.JSON, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_jsonb");
        assertNotNull(row.get(index), "my_jsonb");
        assertEquals(meta.getSQLType(index), PgType.JSONB, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_xml");
        assertNotNull(row.get(index), "my_xml");
        assertEquals(meta.getSQLType(index), PgType.XML, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_gender");
        assertNotNull(row.get(index), "my_gender");
        assertEquals(meta.getSQLType(index), PgType.UNSPECIFIED, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_uuid");
        assertNotNull(row.get(index), "my_uuid");
        assertEquals(meta.getSQLType(index), PgType.UUID, "sqlType");
        assertEquals(meta.getPrecision(index), 16, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_point");
        assertNotNull(row.get(index), "my_point");
        assertEquals(meta.getSQLType(index), PgType.POINT, "sqlType");
        assertEquals(meta.getPrecision(index), 16, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_line");
        assertNotNull(row.get(index), "my_line");
        assertEquals(meta.getSQLType(index), PgType.LINE, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_line_segment");
        assertNotNull(row.get(index), "my_line_segment");
        assertEquals(meta.getSQLType(index), PgType.LINE_SEGMENT, "sqlType");
        assertEquals(meta.getPrecision(index), 32, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");


        index = meta.getColumnIndex("my_box");
        assertNotNull(row.get(index), "my_box");
        assertEquals(meta.getSQLType(index), PgType.BOX, "sqlType");
        assertEquals(meta.getPrecision(index), 32, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_path");
        assertNotNull(row.get(index), "my_path");
        assertEquals(meta.getSQLType(index), PgType.PATH, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_polygon");
        assertNotNull(row.get(index), "my_polygon");
        assertEquals(meta.getSQLType(index), PgType.POLYGON, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_circles");
        assertNotNull(row.get(index), "my_circles");
        assertEquals(meta.getSQLType(index), PgType.CIRCLES, "sqlType");
        assertEquals(meta.getPrecision(index), 24, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_cidr");
        assertNotNull(row.get(index), "my_cidr");
        assertEquals(meta.getSQLType(index), PgType.CIDR, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_inet");
        assertNotNull(row.get(index), "my_inet");
        assertEquals(meta.getSQLType(index), PgType.INET, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_macaddr");
        assertNotNull(row.get(index), "my_macaddr");
        assertEquals(meta.getSQLType(index), PgType.MACADDR, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_macaddr8");
        assertNotNull(row.get(index), "my_macaddr8");
        assertEquals(meta.getSQLType(index), PgType.MACADDR8, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tsvector");
        assertNotNull(row.get(index), "my_tsvector");
        assertEquals(meta.getSQLType(index), PgType.TSVECTOR, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tsquery");
        assertNotNull(row.get(index), "my_tsquery");
        assertEquals(meta.getSQLType(index), PgType.TSQUERY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_int4_range");
        assertNotNull(row.get(index), "my_int4_range");
        assertEquals(meta.getSQLType(index), PgType.INT4RANGE, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_int8_range");
        assertNotNull(row.get(index), "my_int8_range");
        assertEquals(meta.getSQLType(index), PgType.INT8RANGE, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_numrange");
        assertNotNull(row.get(index), "my_numrange");
        assertEquals(meta.getSQLType(index), PgType.NUMRANGE, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tsrange");
        assertNotNull(row.get(index), "my_tsrange");
        assertEquals(meta.getSQLType(index), PgType.TSRANGE, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tstzrange");
        assertNotNull(row.get(index), "my_tstzrange");
        assertEquals(meta.getSQLType(index), PgType.TSTZRANGE, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_daterange");
        assertNotNull(row.get(index), "my_daterange");
        assertEquals(meta.getSQLType(index), PgType.DATERANGE, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_smallint_array");
        assertNotNull(row.get(index), "my_smallint_array");
        assertEquals(meta.getSQLType(index), PgType.SMALLINT_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 2, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_integer_array");
        assertNotNull(row.get(index), "my_integer_array");
        assertEquals(meta.getSQLType(index), PgType.INTEGER_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 4, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_bigint_array");
        assertNotNull(row.get(index), "my_bigint_array");
        assertEquals(meta.getSQLType(index), PgType.BIGINT_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_decimal_array");
        assertNotNull(row.get(index), "my_decimal_array");
        assertEquals(meta.getSQLType(index), PgType.DECIMAL_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_real_array");
        assertNotNull(row.get(index), "my_real_array");
        assertEquals(meta.getSQLType(index), PgType.REAL_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 4, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_double_array");
        assertNotNull(row.get(index), "my_double_array");
        assertEquals(meta.getSQLType(index), PgType.DOUBLE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_boolean_array");
        assertNotNull(row.get(index), "my_boolean_array");
        assertEquals(meta.getSQLType(index), PgType.BOOLEAN_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 1, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_timestamp_array");
        assertNotNull(row.get(index), "my_timestamp_array");
        assertEquals(meta.getSQLType(index), PgType.TIMESTAMP_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_time_array");
        assertNotNull(row.get(index), "my_time_array");
        assertEquals(meta.getSQLType(index), PgType.TIME_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_zoned_timestamp_array");
        assertNotNull(row.get(index), "my_zoned_timestamp_array");
        assertEquals(meta.getSQLType(index), PgType.TIMESTAMPTZ_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 8, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_zoned_time_array");
        assertNotNull(row.get(index), "my_zoned_time_array");
        assertEquals(meta.getSQLType(index), PgType.TIMETZ_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 12, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");


        index = meta.getColumnIndex("my_date_array");
        assertNotNull(row.get(index), "my_date_array");
        assertEquals(meta.getSQLType(index), PgType.DATE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 4, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_bit_array");
        assertNotNull(row.get(index), "my_bit_array");
        assertEquals(meta.getSQLType(index), PgType.BIT_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 64, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_varbit_array");
        assertNotNull(row.get(index), "my_varbit_array");
        assertEquals(meta.getSQLType(index), PgType.VARBIT_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 64, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_interval_array");
        assertNotNull(row.get(index), "my_interval_array");
        assertEquals(meta.getSQLType(index), PgType.INTERVAL_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 16, "precision");
        assertEquals(meta.getScale(index), 6, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_bytea_array");
        assertNotNull(row.get(index), "my_bytea_array");
        assertEquals(meta.getSQLType(index), PgType.BYTEA_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_money_array");
        assertNotNull(row.get(index), "my_money_array");
        assertEquals(meta.getSQLType(index), PgType.MONEY_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_varchar_array");
        assertNotNull(row.get(index), "my_varchar_array");
        assertEquals(meta.getSQLType(index), PgType.VARCHAR_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 225, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_char_array");
        assertNotNull(row.get(index), "my_char_array");
        assertEquals(meta.getSQLType(index), PgType.CHAR_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 225, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_text_array");
        assertNotNull(row.get(index), "my_text_array");
        assertEquals(meta.getSQLType(index), PgType.TEXT_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_json_array");
        assertNotNull(row.get(index), "my_json_array");
        assertEquals(meta.getSQLType(index), PgType.JSON_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_jsonb_array");
        assertNotNull(row.get(index), "my_jsonb_array");
        assertEquals(meta.getSQLType(index), PgType.JSONB_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_xml_array");
        assertNotNull(row.get(index), "my_xml_array");
        assertEquals(meta.getSQLType(index), PgType.XML_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_uuid_array");
        assertNotNull(row.get(index), "my_uuid_array");
        assertEquals(meta.getSQLType(index), PgType.UUID_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 16, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_point_array");
        assertNotNull(row.get(index), "my_point_array");
        assertEquals(meta.getSQLType(index), PgType.POINT_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 16, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_line_array");
        assertNotNull(row.get(index), "my_line_array");
        assertEquals(meta.getSQLType(index), PgType.LINE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_line_segment_array");
        assertNotNull(row.get(index), "my_line_segment_array");
        assertEquals(meta.getSQLType(index), PgType.LINE_SEGMENT_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 32, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_box_array");
        assertNotNull(row.get(index), "my_box_array");
        assertEquals(meta.getSQLType(index), PgType.BOX_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 32, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_path_array");
        assertNotNull(row.get(index), "my_path_array");
        assertEquals(meta.getSQLType(index), PgType.PATH_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_polygon_array");
        assertNotNull(row.get(index), "my_polygon_array");
        assertEquals(meta.getSQLType(index), PgType.POLYGON_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_circles_array");
        assertNotNull(row.get(index), "my_circles_array");
        assertEquals(meta.getSQLType(index), PgType.CIRCLES_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), 24, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_cidr_array");
        assertNotNull(row.get(index), "my_cidr_array");
        assertEquals(meta.getSQLType(index), PgType.CIDR_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_inet_array");
        assertNotNull(row.get(index), "my_inet_array");
        assertEquals(meta.getSQLType(index), PgType.INET_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_macaddr_array");
        assertNotNull(row.get(index), "my_macaddr_array");
        assertEquals(meta.getSQLType(index), PgType.MACADDR_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");


        index = meta.getColumnIndex("my_macaddr8_array");
        assertNotNull(row.get(index), "my_macaddr8_array");
        assertEquals(meta.getSQLType(index), PgType.MACADDR8_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tsvector_array");
        assertNotNull(row.get(index), "my_tsvector_array");
        assertEquals(meta.getSQLType(index), PgType.TSVECTOR_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tsquery_array");
        assertNotNull(row.get(index), "my_tsquery_array");
        assertEquals(meta.getSQLType(index), PgType.TSQUERY_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertTrue(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");


        index = meta.getColumnIndex("my_int4_range_array");
        assertNotNull(row.get(index), "my_int4_range_array");
        assertEquals(meta.getSQLType(index), PgType.INT4RANGE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_int8_range_array");
        assertNotNull(row.get(index), "my_int8_range_array");
        assertEquals(meta.getSQLType(index), PgType.INT8RANGE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_numrange_array");
        assertNotNull(row.get(index), "my_numrange_array");
        assertEquals(meta.getSQLType(index), PgType.NUMRANGE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tsrange_array");
        assertNotNull(row.get(index), "my_tsrange_array");
        assertEquals(meta.getSQLType(index), PgType.TSRANGE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_tstzrange_array");
        assertNotNull(row.get(index), "my_tstzrange_array");
        assertEquals(meta.getSQLType(index), PgType.TSTZRANGE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");

        index = meta.getColumnIndex("my_daterange_array");
        assertNotNull(row.get(index), "my_daterange_array");
        assertEquals(meta.getSQLType(index), PgType.DATERANGE_ARRAY, "sqlType");
        assertEquals(meta.getPrecision(index), UNKNOWN_PRECISION, "precision");
        assertEquals(meta.getScale(index), UNKNOWN_SCALE, "scale");
        assertFalse(meta.getSQLType(index).isCaseSensitive(), "caseSensitive");
    }

    private static void assertUnknownFieldTypeMeta(final ResultRowMeta meta, final int index) {

        try {
            assertEquals(meta.getFieldType(index), FieldType.UNKNOWN, "fieldType");
            assertNull(meta.getCatalogName(index), "catalogName");
            assertNull(meta.getSchemaName(index), "schemaName");
            assertNull(meta.getTableName(index), "tableName");

            assertNull(meta.getColumnName(index), "columnName");
            assertEquals(meta.getKeyMode(index), KeyMode.UNKNOWN, "keyMode");
            assertFalse(meta.isAutoIncrement(index), "autoIncrement");

        } catch (Throwable e) {
            throw new RuntimeException(meta.getColumnLabel(index), e);
        }
    }


}
