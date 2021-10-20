package io.jdbd.mysql.session;

import io.jdbd.DriverManager;
import io.jdbd.mysql.protocol.client.ClientTestUtils;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.TxDatabaseSession;
import io.jdbd.stmt.StaticStatement;
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
        final TxDatabaseSession session;
        session = Mono.from(factory.getTxSession())
                .block();
        assertNotNull(session, "session");
        final StaticStatement statement = session.statement();
        Mono.from(statement.executeUpdate("SET @@SESSION.session_track_transaction_info='CHARACTERISTICS'"))
                .then(Mono.from(statement.executeUpdate("SET autocommit = 0")))
//                .thenMany(statement.executeQuery("SELECT 1 AS result"))
//                .then(Mono.from(statement.executeUpdate("UPDATE mysql_types AS t SET t.my_time = '00:01:00' WHERE t.id = 3")))
//                .then(Mono.from((statement.executeUpdate("INSERT INTO mysql_types(name) VALUES('army')"))))
//                .then(Mono.from((statement.executeUpdate("INSERT INTO mysql_types(name) VALUES('army')"))))
                .then(Mono.from((statement.executeUpdate("COMMIT"))))
                .then()
                .block();

    }


}
