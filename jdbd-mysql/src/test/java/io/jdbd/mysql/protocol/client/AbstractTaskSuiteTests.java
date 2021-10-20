package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.session.SessionAdjutant;
import io.jdbd.session.DatabaseSessionFactory;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.resources.LoopResources;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskSuiteTests.class);

    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-mysql", 20, true)
            .onClient(true);

    static final long TIME_OUT = 5 * 1000L;

    static final Queue<TaskAdjutant> TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

    static final SessionAdjutant DEFAULT_SESSION_ADJUTANT = createDefaultSessionAdjutant();

    protected static final String PROTOCOL_KEY = "my$protocol";


    protected static EventLoopGroup getEventLoopGroup() {
        return EVENT_LOOP_GROUP;
    }


    protected static TaskAdjutant obtainTaskAdjutant() {
        throw new UnsupportedOperationException();
    }


    protected static void releaseConnection(TaskAdjutant adjutant) {
        TASK_ADJUTANT_QUEUE.offer(adjutant);
    }


    protected static SessionAdjutant createSessionAdjutantForSingleHost(Map<String, String> propMap) {
        return new SessionAdjutantForSingleHostTest(ClientTestUtils.singleUrl(propMap));
    }

    static TaskAdjutant getTaskAdjutant(ClientProtocol clientProtocol) {
        return ((ClientProtocolImpl) clientProtocol).adjutant;
    }


    /*################################## blow private method ##################################*/

    private static SessionAdjutant createDefaultSessionAdjutant() {
        final Map<String, String> map;
        map = ClientTestUtils.loadConfigMap();
        map.put("sslMode", Enums.SslMode.DISABLED.name());
        return new SessionAdjutantForSingleHostTest(MySQLUrl.getInstance(map.get("url"), map));
    }


    private static final class SessionAdjutantForSingleHostTest implements SessionAdjutant {

        private final MySQLUrl mySQLUrl;

        private final Map<String, Class<? extends AuthenticationPlugin>> pluginClassMap;

        private SessionAdjutantForSingleHostTest(MySQLUrl mySQLUrl) {
            this.mySQLUrl = mySQLUrl;
            this.pluginClassMap = PluginUtils.createPluginClassMap(mySQLUrl.getPrimaryHost().getProperties());
        }

        @Override
        public MySQLUrl jdbcUrl() {
            return this.mySQLUrl;
        }

        @Override
        public Map<String, Class<? extends AuthenticationPlugin>> pluginClassMap() {
            return this.pluginClassMap;
        }

        @Override
        public Map<String, Charset> customCharsetMap() {
            return Collections.emptyMap();
        }

        @Override
        public EventLoopGroup eventLoopGroup() {
            return EVENT_LOOP_GROUP;
        }


        @Override
        public boolean isSameFactory(DatabaseSessionFactory factory) {
            throw new UnsupportedOperationException();
        }
    }


}
