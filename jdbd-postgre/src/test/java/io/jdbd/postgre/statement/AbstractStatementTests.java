package io.jdbd.postgre.statement;

import io.jdbd.DriverManager;
import io.jdbd.pool.PoolLocalDatabaseSession;
import io.jdbd.postgre.ClientTestUtils;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.testng.Assert.assertNotNull;

abstract class AbstractStatementTests {

    static final DatabaseSessionFactory DEFAULT_FACTORY = createDefaultSessionFactory();

    static final Queue<LocalDatabaseSession> SESSION_QUEUE = new LinkedBlockingQueue<>();

    static final long TIME_OUT = 5000L;

    final LocalDatabaseSession getSession() {
        LocalDatabaseSession session;

        session = SESSION_QUEUE.poll();
        if (session == null) {
            session = Mono.from(DEFAULT_FACTORY.localSession())
                    .block();
            assertNotNull(session, "session");
        }
        return session;
    }

    void closeSession(LocalDatabaseSession session) {
        if (session instanceof PoolLocalDatabaseSession) {
            Mono.from(((PoolLocalDatabaseSession) session).reset())
                    .map(SESSION_QUEUE::offer)
                    .subscribe();
        } else {
            Mono.from(session.close())
                    .subscribe();
        }
    }


    private static DatabaseSessionFactory createDefaultSessionFactory() {
        final Map<String, String> map = ClientTestUtils.loadTestConfigMap();
        final String url = map.remove("url");
        if (url == null) {
            throw new IllegalStateException("No found url in config file.");
        }
        // invoke forPoolVendor for test
        return DriverManager.forPoolVendor(url, map);

    }


}
