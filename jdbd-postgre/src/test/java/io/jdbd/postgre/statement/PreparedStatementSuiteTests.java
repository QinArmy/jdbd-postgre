package io.jdbd.postgre.statement;

import io.jdbd.TxDatabaseSession;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertNotNull;

/**
 * This class is test class of {@code io.jdbd.postgre.session.PgPreparedStatement}
 */
public class PreparedStatementSuiteTests extends AbstractStatementTests {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementSuiteTests.class);

    private static final long START_ID = 400L;

    /**
     * @see PreparedStatement#executeUpdate()
     */
    @Test
    public void prepareNoParamPlaceholder() {
        final TxDatabaseSession session;
        session = getSession();
        final long id = START_ID + 1;
        final String sql = String.format("SELECT t.* FROM my_types AS t WHERE t.id = %s", id);

        final PreparedStatement statement;
        statement = Mono.from(session.prepare(sql))
                .block();
        assertNotNull(statement, "statement");
        final AtomicReference<ResultStates> statesHolder = new AtomicReference<>(null);

        final List<ResultRow> rowList;
        rowList = Flux.from(statement.executeQuery(statesHolder::set))
                .collectList()
                .block();

        assertNotNull(rowList, "rowList");

    }


}
