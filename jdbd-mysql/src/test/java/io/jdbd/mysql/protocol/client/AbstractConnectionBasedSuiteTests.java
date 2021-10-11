package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.session.SessionAdjutant;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    static final Queue<TaskAdjutant> TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

    private static final SessionAdjutant DEFAULT_SESSION_ADJUTANT = createDefaultSessionAdjutant();

    protected static final String PROTOCOL_KEY = "my$protocol";


    protected static EventLoopGroup getEventLoopGroup() {
        return EVENT_LOOP_GROUP;
    }


    protected static TaskAdjutant obtainTaskAdjutant() {
        TaskAdjutant taskAdjutant;

        taskAdjutant = TASK_ADJUTANT_QUEUE.poll();
        if (taskAdjutant == null) {

            ClientConnectionProtocolImpl protocol = ClientConnectionProtocolImpl.create(0, DEFAULT_SESSION_ADJUTANT)
                    .block();
            assertNotNull(protocol, "protocol");

            taskAdjutant = protocol.taskExecutor.taskAdjutant();
        }

        return taskAdjutant;
    }


    protected static void releaseConnection(TaskAdjutant adjutant) {
        TASK_ADJUTANT_QUEUE.offer(adjutant);
    }


    protected static SessionAdjutant createSessionAdjutantForSingleHost(Map<String, String> propMap) {
        return new SessionAdjutantForSingleHostTest(ClientTestUtils.singleUrl(propMap));
    }


    /*################################## blow private method ##################################*/

    private static SessionAdjutant createDefaultSessionAdjutant() {
        Map<String, String> map = new HashMap<>();
        if (ClientTestUtils.existsServerPublicKey()) {
            map.put(MyKey.sslMode.getKey(), Enums.SslMode.DISABLED.name());
        }
        ClientTestUtils.appendZoneConfig(map);
        return createSessionAdjutantForSingleHost(map);
    }


    private static final class SessionAdjutantForSingleHostTest implements SessionAdjutant {

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
        public EventLoopGroup getEventLoopGroup() {
            return EVENT_LOOP_GROUP;
        }

        @Override
        public int maxAllowedPayload() {
            return 0;
        }

    }


}
