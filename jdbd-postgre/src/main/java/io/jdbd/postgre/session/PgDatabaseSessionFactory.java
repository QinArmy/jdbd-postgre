package io.jdbd.postgre.session;

import io.jdbd.ProductFamily;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.config.PgKey;
import io.jdbd.postgre.config.PostgreUrl;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.postgre.protocol.client.ClientProtocolFactory;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.TxDatabaseSession;
import io.jdbd.session.XaDatabaseSession;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.util.Map;

public class PgDatabaseSessionFactory implements DatabaseSessionFactory {

    /**
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    public static PgDatabaseSessionFactory create(final String url, final Map<String, String> properties) {
        final PostgreUrl postgreUrl;
        postgreUrl = PostgreUrl.create(url, properties);
        return new PgDatabaseSessionFactory(postgreUrl, false);
    }

    /**
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    public static PgDatabaseSessionFactory forPoolVendor(final String url, final Map<String, String> properties) {
        final PostgreUrl postgreUrl;
        postgreUrl = PostgreUrl.create(url, properties);
        return new PgDatabaseSessionFactory(postgreUrl, true);
    }


    public static boolean acceptsUrl(String url) {
        return PostgreUrl.acceptsUrl(url);
    }

    private final PostgreUrl postgreUrl;

    private final boolean forPoolVendor;

    private final PgSessionAdjutant sessionAdjutant;


    private PgDatabaseSessionFactory(PostgreUrl postgreUrl, boolean forPoolVendor) {
        this.postgreUrl = postgreUrl;
        this.forPoolVendor = forPoolVendor;
        this.sessionAdjutant = new PgSessionAdjutant(this);
    }


    @Override
    public Mono<TxDatabaseSession> getTxSession() {
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createTxSession);
    }

    @Override
    public Mono<XaDatabaseSession> getXaSession() {
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createXaSession);
    }


    @Override
    public int getMajorVersion() {
        return PgDriver.getMajorVersion();

    }

    @Override
    public int getMinorVersion() {
        return PgDriver.getMinorVersion();
    }

    @Override
    public String getDriverName() {
        return PgDriver.getName();
    }

    @Override
    public ProductFamily getProductFamily() {
        return ProductFamily.Postgre;
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #getTxSession()
     */
    private TxDatabaseSession createTxSession(final ClientProtocol protocol) {
        final TxDatabaseSession session;
        if (this.forPoolVendor) {
            session = PgTxDatabaseSession.forPoolVendor(this.sessionAdjutant, protocol);
        } else {
            session = PgTxDatabaseSession.create(this.sessionAdjutant, protocol);
        }
        return session;
    }

    /**
     * @see #getXaSession()
     */
    private XaDatabaseSession createXaSession(ClientProtocol protocol) {
        final XaDatabaseSession session;
        if (this.forPoolVendor) {
            session = PgXaDatabaseSession.forPoolVendor(this.sessionAdjutant, protocol);
        } else {
            session = PgXaDatabaseSession.create(this.sessionAdjutant, protocol);
        }
        return session;
    }


    private static final class PgSessionAdjutant implements SessionAdjutant {

        private final PgDatabaseSessionFactory factory;

        private final EventLoopGroup eventLoopGroup;

        private PgSessionAdjutant(PgDatabaseSessionFactory factory) {
            this.factory = factory;
            final int workerCount = factory.postgreUrl.getOrDefault(PgKey.factoryWorkerCount, Integer.class);
            this.eventLoopGroup = LoopResources.create("jdbd-postgre", workerCount, true)
                    .onClient(true);
        }

        @Override
        public PostgreUrl getJdbcUrl() {
            return this.factory.postgreUrl;
        }

        @Override
        public EventLoopGroup getEventLoopGroup() {
            return this.eventLoopGroup;
        }

        @Override
        public boolean isSameFactory(DatabaseSessionFactory factory) {
            return factory == this.factory;
        }
    }


}
