package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.session.MySQLSessionAdjutant;
import io.netty.channel.EventLoopGroup;
import reactor.netty.resources.LoopResources;

import java.util.Map;

public abstract class AbstractConnectionBasedSuiteTests {


    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-mysql", 20, true)
            .onClient(true);

    static final long TIME_OUT = 5L * 1000L;


    protected static EventLoopGroup getEventLoopGroup() {
        return EVENT_LOOP_GROUP;
    }

    protected static MySQLSessionAdjutant getSessionAdjutantForSingleHost(Map<String, String> propMap) {
        return new SessionAdjutantForSingleHostTest(ClientTestUtils.singleUrl(propMap));
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
