package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Group;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.postgre.util.PgStreams;
import io.jdbd.result.ResultState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(groups = {Group.TASK_TEST_ADVICE}, dependsOnGroups = {Group.URL, Group.PARSER, Group.UTILS, Group.SESSION_BUILDER})
public class TaskTestAdvice extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(TaskTestAdvice.class);

    @BeforeClass
    public static void beforeClass() throws IOException {
        List<String> sqlList = new ArrayList<>(4);
        sqlList.add("DROP TABLE IF EXISTS my_types");
        sqlList.add("DROP TYPE IF EXISTS gender");
        sqlList.add("CREATE TYPE gender AS ENUM ('FEMALE','MALE','UNKNOWN')");
        sqlList.add(PgStreams.readAsString(Paths.get(ClientTestUtils.getTestResourcesPath().toString(), "data/sql/my_types.sql")));

        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long resultCount;
        resultCount = SimpleQueryTask.batchUpdate(PgStmts.group(sqlList), adjutant)
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

        final ClientProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final ResultState state;
        state = SimpleQueryTask.update(PgStmts.stmt(builder.toString()), adjutant)
                .switchIfEmpty(PgTestUtils.updateNoResponse())
                .concatWith(TaskTestAdvice.releaseConnection(protocol))
                .blockLast();

        assertNotNull(state, "state");
        assertEquals(state.getAffectedRows(), rowCount, "affectedRows");

    }


}
