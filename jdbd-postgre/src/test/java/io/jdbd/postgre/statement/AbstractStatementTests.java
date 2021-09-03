package io.jdbd.postgre.statement;

import io.jdbd.DatabaseSessionFactory;
import io.jdbd.DriverManager;
import io.jdbd.TxDatabaseSession;
import io.jdbd.postgre.ClientTestUtils;
import io.jdbd.postgre.PgDriver;
import reactor.core.publisher.Mono;

import java.util.Map;

import static org.testng.Assert.assertNotNull;

abstract class AbstractStatementTests {

    static final DatabaseSessionFactory DEFAULT_FACTORY = createDefaultSessionFactory();


    TxDatabaseSession getSession() {
        final TxDatabaseSession session;
        session = Mono.from(DEFAULT_FACTORY.getTxSession())
                .block();
        assertNotNull(session, "session");
        return session;
    }


    private static DatabaseSessionFactory createDefaultSessionFactory() {
        final Map<String, String> map = ClientTestUtils.loadTestConfigMap();
        final String url = map.remove("url");
        if (url == null) {
            throw new IllegalStateException("No found url in config file.");
        }
        DriverManager.registerDriver(PgDriver.class);
        return DriverManager.createSessionFactory(url, map);

    }


}
