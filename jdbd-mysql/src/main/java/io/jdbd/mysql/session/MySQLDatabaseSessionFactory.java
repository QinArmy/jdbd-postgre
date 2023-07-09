package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.mysql.env.MySQLEnvironment;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.client.ClientProtocolFactory;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
import io.jdbd.vendor.env.Properties;
import io.netty.channel.EventLoopGroup;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class MySQLDatabaseSessionFactory implements DatabaseSessionFactory {

    static final String MY_SQL = "MySQL";

    public static MySQLDatabaseSessionFactory create(String url, Map<String, Object> properties)
            throws JdbdException {
        return new MySQLDatabaseSessionFactory(createProtocolFactory(url, properties), false);
    }

    public static MySQLDatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties)
            throws JdbdException {
        return new MySQLDatabaseSessionFactory(createProtocolFactory(url, properties), true);
    }


    private static MySQLProtocolFactory createProtocolFactory(String url, Map<String, Object> properties) {
        return ClientProtocolFactory.from(MySQLEnvironment.parse(url, properties));
    }


    private final MySQLProtocolFactory protocolFactory;

    private final boolean forPoolVendor;


    private MySQLDatabaseSessionFactory(MySQLProtocolFactory protocolFactory, boolean forPoolVendor) {
        this.protocolFactory = protocolFactory;
        this.forPoolVendor = forPoolVendor;
    }

    @Override
    public Mono<LocalDatabaseSession> localSession() {
        return this.protocolFactory.createProtocol()
                .map(this::createLocalSession);
    }

    @Override
    public Mono<RmDatabaseSession> rmSession() {
        return this.protocolFactory.createProtocol()
                .map(this::createRmSession);
    }


    @Override
    public String productName() {
        return MY_SQL;
    }

    @Override
    public Publisher<Void> close() {
        return this.protocolFactory.close();
    }


    /**
     * @see #localSession()
     */
    private LocalDatabaseSession createLocalSession(final MySQLProtocol protocol) {
        final LocalDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLLocalDatabaseSession.forPoolVendor(this, protocol);
        } else {
            session = MySQLLocalDatabaseSession.create(this, protocol);
        }
        return session;
    }

    /**
     * @see #rmSession()
     */
    private RmDatabaseSession createRmSession(final MySQLProtocol protocol) {
        final RmDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLRmDatabaseSession.forPoolVendor(this, protocol);
        } else {
            session = MySQLRmDatabaseSession.create(this, protocol);
        }
        return session;
    }

    private static final class MySQLSessionAdjutant implements SessionAdjutant {

        private final MySQLDatabaseSessionFactory factory;

        private final EventLoopGroup eventLoopGroup;

        private final Map<String, Charset> customCharsetMap;

        private MySQLSessionAdjutant(MySQLDatabaseSessionFactory factory) {
            this.factory = factory;
            final Properties properties = this.factory.mySQLUrl.getCommonProps();
            final int workerCount = properties.getOrDefault(MyKey.factoryWorkerCount, Integer.class);
            this.eventLoopGroup = LoopResources.create("jdbd-MySQL", workerCount, true)
                    .onClient(true);
            this.customCharsetMap = createCustomCharsetMap(properties);
        }

        @Override
        public MySQLUrl jdbcUrl() {
            return this.factory.mySQLUrl;
        }

        @Override
        public Map<String, Class<? extends AuthenticationPlugin>> pluginClassMap() {
            return PluginUtils.createPluginClassMap(this.factory.mySQLUrl.getCommonProps());
        }

        @Override
        public Map<String, Charset> customCharsetMap() {
            return this.customCharsetMap;
        }

        @Override
        public EventLoopGroup eventLoopGroup() {
            return this.eventLoopGroup;
        }

        @Override
        public boolean isSameFactory(final DatabaseSessionFactory factory) {
            return factory == this.factory;
        }

    }


    /**
     * @return a unmodified map
     * @throws PropertyException when {@link MyKey#customCharsetMapping} value error.
     */
    private static Map<String, Charset> createCustomCharsetMap(final Properties properties) {
        final String mappingValue;
        mappingValue = properties.get(MyKey.customCharsetMapping);

        if (!MySQLStrings.hasText(mappingValue)) {
            return Collections.emptyMap();
        }
        final String[] pairs = mappingValue.split(";");
        String[] valuePair;
        final Map<String, Charset> tempMap = new HashMap<>((int) (pairs.length / 0.75F));
        for (String pair : pairs) {
            valuePair = pair.split(":");
            if (valuePair.length != 2) {
                String m = String.format("%s value format error.", MyKey.customCharsetMapping);
                throw new PropertyException(MyKey.customCharsetMapping.getKey(), m);
            }
            tempMap.put(valuePair[0], Charset.forName(valuePair[1]));
        }
        return Collections.unmodifiableMap(tempMap);
    }


}
