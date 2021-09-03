package io.jdbd.postgre.statement;

import io.jdbd.TxDatabaseSession;
import io.jdbd.stmt.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is test class of {@code io.jdbd.postgre.session.PgPreparedStatement}
 */
public class PreparedStatementSuiteTests extends AbstractStatementTests {

    private static final Logger LOG = LoggerFactory.getLogger(PreparedStatementSuiteTests.class);

    /**
     * @see PreparedStatement#executeUpdate()
     */
    //@Test
    public void executeUpdate() {
        final TxDatabaseSession session;
        session = getSession();
        String sql = "";

        session.prepare(sql);

    }


}
