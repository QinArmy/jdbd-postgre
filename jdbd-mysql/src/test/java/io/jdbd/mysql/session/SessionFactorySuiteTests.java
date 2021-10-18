package io.jdbd.mysql.session;

import io.jdbd.DriverManager;
import io.jdbd.mysql.protocol.client.ClientTestUtils;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.result.ResultRow;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.TxDatabaseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.util.Map;

import static org.testng.Assert.assertNotNull;

/**
 * <p>
 * This class is test class of {@link MySQLDatabaseSessionFactory}.
 * </p>
 */
public class SessionFactorySuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(SessionFactorySuiteTests.class);

    /**
     * @see MySQLDatabaseSessionFactory#getTxSession()
     */
    @Test
    public void getTxSession() {
        final Map<String, String> configMap = ClientTestUtils.loadConfigMap();
        configMap.put(MyKey.sslMode.getKey(), "DISABLED");
        final DatabaseSessionFactory factory;
        factory = DriverManager.forPoolVendor(configMap.get("url"), configMap);

        final ResultRow row;
        row = Mono.from(factory.getTxSession())
                .map(TxDatabaseSession::statement)
                .flatMapMany(statement -> statement.executeQuery("SELECT 1 as result"))
                .elementAt(0)
                .block();
        assertNotNull(row, "row");

        LOG.info("getTxSession test result:{}", row.getNonNull("result", String.class));
    }


}
