package io.jdbd.mysql.session;

import io.jdbd.ProductFamily;
import io.jdbd.config.PropertyException;
import io.jdbd.config.UrlException;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.authentication.PluginUtils;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.protocol.client.ClientProtocolFactory;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.TxDatabaseSession;
import io.jdbd.session.XaDatabaseSession;
import io.jdbd.vendor.conf.Properties;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class MySQLDatabaseSessionFactory implements DatabaseSessionFactory {

    public static MySQLDatabaseSessionFactory create(String url, Map<String, String> properties)
            throws UrlException, PropertyException {
        return new MySQLDatabaseSessionFactory(MySQLUrl.getInstance(url, properties), false);
    }

    public static MySQLDatabaseSessionFactory forPoolVendor(String url, Map<String, String> properties)
            throws UrlException, PropertyException {
        return new MySQLDatabaseSessionFactory(MySQLUrl.getInstance(url, properties), true);
    }


    private final MySQLUrl mySQLUrl;

    private final boolean forPoolVendor;

    private final MySQLSessionAdjutant adjutant;

    private MySQLDatabaseSessionFactory(MySQLUrl mySQLUrl, boolean forPoolVendor) {
        this.mySQLUrl = mySQLUrl;
        this.forPoolVendor = forPoolVendor;
        this.adjutant = new MySQLSessionAdjutant(this);
    }

    @Override
    public Mono<TxDatabaseSession> getTxSession() {
        return getClientProtocol()
                .map(this::createTxSession);
    }

    @Override
    public Mono<XaDatabaseSession> getXaSession() {
        return getClientProtocol()
                .map(this::createXaSession);
    }

    @Override
    public int getMajorVersion() {
        return MySQLDriver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return MySQLDriver.getMinorVersion();
    }

    @Override
    public String getDriverName() {
        return MySQLDriver.getName();
    }

    @Override
    public ProductFamily getProductFamily() {
        return ProductFamily.MySQL;
    }


    private Mono<ClientProtocol> getClientProtocol() {
        final Mono<ClientProtocol> protocolMono;
        switch (mySQLUrl.protocolType) {
            case SINGLE_CONNECTION:
                protocolMono = ClientProtocolFactory.single(this.adjutant);
                break;
            case FAILOVER_CONNECTION:
            case FAILOVER_DNS_SRV_CONNECTION:
            case LOADBALANCE_CONNECTION:
            case LOADBALANCE_DNS_SRV_CONNECTION:
            case REPLICATION_CONNECTION:
            case REPLICATION_DNS_SRV_CONNECTION:
            default:
                throw MySQLExceptions.createUnexpectedEnumException(mySQLUrl.protocolType);
        }
        return protocolMono;
    }

    /**
     * @see #getTxSession()
     */
    private TxDatabaseSession createTxSession(final ClientProtocol protocol) {
        final TxDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLTxDatabaseSession.forPoolVendor(this.adjutant, protocol);
        } else {
            session = MySQLTxDatabaseSession.create(this.adjutant, protocol);
        }
        return session;
    }

    /**
     * @see #getXaSession()
     */
    private XaDatabaseSession createXaSession(ClientProtocol protocol) {
        final XaDatabaseSession session;
        if (this.forPoolVendor) {
            session = MySQLXaDatabaseSession.forPoolVendor(this.adjutant, protocol);
        } else {
            session = MySQLXaDatabaseSession.create(this.adjutant, protocol);
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
