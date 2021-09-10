package io.jdbd.postgre.statement;

import io.jdbd.TxDatabaseSession;
import io.jdbd.postgre.PgTestUtils;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;

/**
 * This class is test class of {@code io.jdbd.postgre.session.PgPreparedStatement}
 */
public class PreparedStatementSuiteTests extends AbstractStatementTests {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementSuiteTests.class);

    private static final long START_ID = 400L;

    @Test
    public void updateWithoutParameter() {
        final TxDatabaseSession session;
        session = getSession();
        final long id = START_ID + 1;
        final String sql = String.format("UPDATE my_types AS t SET my_boolean = TRUE WHERE t.id = %s", id);

        final ResultStates states;
        states = Mono.from(session.prepare(sql))
                .flatMap(statement -> Mono.from(statement.executeUpdate()))
                .block();

        assertNotNull(states, "states");
        PgTestUtils.assertUpdateOneWithoutMoreResult(states);
    }

    /**
     * @see PreparedStatement#executeUpdate()
     */
    @Test
    public void queryWithoutParameter() {
        final TxDatabaseSession session;
        session = getSession();
        final long id = START_ID + 2;
        final String sql = String.format("SELECT t.* FROM my_types AS t WHERE t.id = %s", id);

        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        final List<ResultRow> rowList;

        rowList = Mono.from(session.prepare(sql))
                .flatMapMany(statement -> statement.executeQuery(statesHolder::set))
                .collectList()
                .block();

        assertNotNull(rowList, "rowList");
        assertFalse(rowList.isEmpty(), "rowList is empty.");

        ResultStates states = statesHolder.get();
        assertNotNull(states, "ResultStates");
        assertEquals(states.getRowCount(), 1, "rowCount");
    }


}
