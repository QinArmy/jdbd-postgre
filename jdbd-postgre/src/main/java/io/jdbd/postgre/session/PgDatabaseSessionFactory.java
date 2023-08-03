package io.jdbd.postgre.session;

import io.jdbd.DriverVersion;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.env.PgKey;
import io.jdbd.postgre.env.PgUrl;
import io.jdbd.postgre.protocol.client.ClientProtocolFactory;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
import io.netty.channel.EventLoopGroup;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.util.Map;

public class PgDatabaseSessionFactory implements DatabaseSessionFactory {

    /**
     * @throws UrlException      when url error.
     * @throws PropertyException when properties error.
     */
    public static PgDatabaseSessionFactory create(final String url, final Map<String, String> properties) {
        final PgUrl pgUrl;
        pgUrl = PgUrl.create(url, properties);
        return new PgDatabaseSessionFactory(pgUrl, false);
    }

    /**
     * @throws UrlException      when url error.
     * @throws PropertyException when properties error.
     */
    public static PgDatabaseSessionFactory forPoolVendor(final String url, final Map<String, String> properties) {
        final PgUrl pgUrl;
        pgUrl = PgUrl.create(url, properties);
        return new PgDatabaseSessionFactory(pgUrl, true);
    }


    public static boolean acceptsUrl(String url) {
        return PgUrl.acceptsUrl(url);
    }

    private final PgUrl pgUrl;

    private final boolean forPoolVendor;

    private final PgSessionAdjutant sessionAdjutant;


    private PgDatabaseSessionFactory(PgUrl pgUrl, boolean forPoolVendor) {
        this.pgUrl = pgUrl;
        this.forPoolVendor = forPoolVendor;
        this.sessionAdjutant = new PgSessionAdjutant(this);
    }


    @Override
    public Mono<LocalDatabaseSession> localSession() {
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createTxSession);
    }

    @Override
    public Mono<RmDatabaseSession> rmSession() {
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createXaSession);
    }

    @Override
    public DriverVersion getDriverVersion() {
        return PgDriver.getVersion();
    }

    @Override
    public ProductFamily getProductFamily() {
        return ProductFamily.Postgre;
    }

    @Override
    public Publisher<Void> close() {
        return null;
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #localSession()
     */
    private LocalDatabaseSession createTxSession(final PgProtocol protocol) {
        final LocalDatabaseSession session;
        if (this.forPoolVendor) {
            session = PgLocalDatabaseSession.forPoolVendor(this.sessionAdjutant, protocol);
        } else {
            session = PgLocalDatabaseSession.create(this.sessionAdjutant, protocol);
        }
        return session;
    }

    /**
     * @see #rmSession()
     */
    private RmDatabaseSession createXaSession(PgProtocol protocol) {
        final RmDatabaseSession session;
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
            final int workerCount = factory.pgUrl.getOrDefault(PgKey.factoryWorkerCount, Integer.class);
            this.eventLoopGroup = LoopResources.create("jdbd-postgre", workerCount, true)
                    .onClient(true);
        }

        @Override
        public PgUrl jdbcUrl() {
            return this.factory.pgUrl;
        }

        @Override
        public EventLoopGroup eventLoopGroup() {
            return this.eventLoopGroup;
        }

        @Override
        public boolean isSameFactory(DatabaseSessionFactory factory) {
            return factory == this.factory;
        }
    }


}
