package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.testng.Assert.assertNotNull;

public abstract class AbstractConnectionBasedSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractConnectionBasedSuiteTests.class);

    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-mysql", 20, true)
            .onClient(true);

    static final long TIME_OUT = 5 * 1000L;

    static final Queue<MySQLTaskAdjutant> TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

    static final Queue<MySQLTaskAdjutant> MULTI_STMT_TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

    private static final MySQLSessionAdjutant DEFAULT_SESSION_ADJUTANT = createDefaultSessionAdjutant();

    private static final MySQLSessionAdjutant MULTI_STMT_SESSION_ADJUTANT = createMultiStmtSessionAdjutant();

    protected static final String PROTOCOL_KEY = "my$protocol";


    protected static EventLoopGroup getEventLoopGroup() {
        return EVENT_LOOP_GROUP;
    }

    protected MySQLTaskAdjutant obtainTaskAdjutant() {
        MySQLTaskAdjutant taskAdjutant;

        taskAdjutant = TASK_ADJUTANT_QUEUE.poll();
        if (taskAdjutant == null) {

            ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, DEFAULT_SESSION_ADJUTANT)
                    .block();
            assertNotNull(protocol, "protocol");

            taskAdjutant = protocol.taskExecutor.getAdjutant();
        }

        return taskAdjutant;
    }

    protected MySQLTaskAdjutant obtainMultiStmtTaskAdjutant() {
        MySQLTaskAdjutant taskAdjutant;

        taskAdjutant = MULTI_STMT_TASK_ADJUTANT_QUEUE.poll();
        if (taskAdjutant == null) {

            ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, MULTI_STMT_SESSION_ADJUTANT)
                    .block();
            assertNotNull(protocol, "protocol");

            taskAdjutant = protocol.taskExecutor.getAdjutant();
        }

        return taskAdjutant;
    }


    protected void releaseConnection(MySQLTaskAdjutant adjutant) {
        TASK_ADJUTANT_QUEUE.add(adjutant);
    }

    protected void releaseMultiStmtConnection(MySQLTaskAdjutant adjutant) {
        MULTI_STMT_TASK_ADJUTANT_QUEUE.add(adjutant);
    }


    protected static MySQLSessionAdjutant getSessionAdjutantForSingleHost(Map<String, String> propMap) {
        return new SessionAdjutantForSingleHostTest(ClientTestUtils.singleUrl(propMap));
    }

    protected static Mono<Void> quitConnection(MySQLTaskAdjutant adjutant) {
        return QuitTask.quit(adjutant);
    }

    /*################################## blow private method ##################################*/

    private static MySQLSessionAdjutant createDefaultSessionAdjutant() {
        Map<String, String> map = new HashMap<>();
        if (ClientTestUtils.existsServerPublicKey()) {
            map.put(PropertyKey.sslMode.getKey(), Enums.SslMode.DISABLED.name());
        }
        ClientTestUtils.appendZoneConfig(map);

        return getSessionAdjutantForSingleHost(map);
    }

    private static MySQLSessionAdjutant createMultiStmtSessionAdjutant() {
        Map<String, String> map = new HashMap<>();
        if (ClientTestUtils.existsServerPublicKey()) {
            map.put(PropertyKey.sslMode.getKey(), Enums.SslMode.DISABLED.name());
        }
        ClientTestUtils.appendZoneConfig(map);
        map.put(PropertyKey.allowMultiQueries.getKey(), "true");
        return getSessionAdjutantForSingleHost(map);
    }


    private static final class SessionAdjutantForSingleHostTest implements MySQLSessionAdjutant {

        private final MySQLUrl mySQLUrl;

        private final Map<String, Class<? extends AuthenticationPlugin>> pluginClassMap;

        private SessionAdjutantForSingleHostTest(MySQLUrl mySQLUrl) {
            this.mySQLUrl = mySQLUrl;
            this.pluginClassMap = PluginUtils.createPluginClassMap(mySQLUrl.getPrimaryHost().getProperties());
        }

        @Override
        public MySQLUrl obtainUrl() {
            return this.mySQLUrl;
        }

        @Override
        public Map<String, Class<? extends AuthenticationPlugin>> obtainPluginClassMap() {
            return this.pluginClassMap;
        }

        @Override
        public EventLoopGroup obtainEventLoopGroup() {
            return EVENT_LOOP_GROUP;
        }
    }


}
