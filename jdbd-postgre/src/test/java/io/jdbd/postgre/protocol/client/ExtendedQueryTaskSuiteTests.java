package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.PgStmts;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * This class is test class of the methods of  {@link ExtendedQueryTask}.
 */
public class ExtendedQueryTaskSuiteTests extends AbstractTaskTests {

    private static final Logger LOG = LoggerFactory.getLogger(ExtendedQueryTaskSuiteTests.class);

    private static final long START_ID = 400L;

    /**
     * @see ExtendedQueryTask#query(BindStmt, TaskAdjutant)
     */
    @Test
    public void updateForOneShot() {
        final PgProtocol protocol;
        protocol = obtainProtocolWithSync();
        final TaskAdjutant adjutant = mapToTaskAdjutant(protocol);

        final long id = START_ID + 1;
        final String sql = String.format("SELECT t.* FROM my_types AS t WHERE t.id = %s", id);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        final BindStmt stmt = PgStmts.bind(sql, Collections.emptyList(), statesHolder::set);
        final List<ResultRow> rowList;
        rowList = ExtendedQueryTask.query(stmt, adjutant)

                .concatWith(releaseConnection(protocol))
                .onErrorResume(releaseConnectionOnError(protocol))

                .collectList()
                .block();

        assertNotNull(rowList, "rowList");
        assertEquals(rowList.size(), 1, "rowList size");
    }


}
