package io.jdbd.mysql.session;

import io.jdbd.ProductFamily;
import io.jdbd.config.PropertyException;
import io.jdbd.config.UrlException;
import io.jdbd.mysql.MySQLDriver;
import io.jdbd.mysql.protocol.authentication.AuthenticationPlugin;
import io.jdbd.mysql.protocol.client.ClientProtocol;
import io.jdbd.mysql.protocol.client.ClientProtocolFactory;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.TxDatabaseSession;
import io.jdbd.session.XaDatabaseSession;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

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

        private MySQLSessionAdjutant(MySQLDatabaseSessionFactory factory) {
            this.factory = factory;
            final int workerCount = 100;
            this.eventLoopGroup = LoopResources.create("jdbd-MySQL", workerCount, true)
                    .onClient(true);
        }

        @Override
        public MySQLUrl getJdbcUrl() {
            return this.factory.mySQLUrl;
        }

        @Override
        public Map<String, Class<? extends AuthenticationPlugin>> obtainPluginClassMap() {
            return null;
        }

        @Override
        public int maxAllowedPayload() {
            return 0;
        }

        @Override
        public EventLoopGroup getEventLoopGroup() {
            return null;
        }

        @Override
        public boolean isSameFactory(DatabaseSessionFactory factory) {
            return false;
        }

    }


}
