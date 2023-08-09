package io.jdbd.postgre.session;

import io.jdbd.postgre.env.PgKey;
import io.jdbd.postgre.env.PgUrl;
import io.jdbd.postgre.protocol.client.ClientProtocolFactory;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
import io.netty.channel.EventLoopGroup;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PgDatabaseSessionFactory implements DatabaseSessionFactory {

    /**
     * @throws UrlException      when url error.
     * @throws PropertyException when properties error.
     */
    public static PgDatabaseSessionFactory create(final String url, final Map<String, String> properties,
                                                  final boolean pool) {
        final PgUrl pgUrl;
        pgUrl = PgUrl.create(url, properties);
        return new PgDatabaseSessionFactory(pgUrl, false);
    }


    public static boolean acceptsUrl(String url) {
        return PgUrl.acceptsUrl(url);
    }

    private final PgUrl pgUrl;

    private String name;

    private final boolean forPoolVendor;

    private final PgSessionAdjutant sessionAdjutant;

    private final AtomicBoolean closed = new AtomicBoolean(false);


    private PgDatabaseSessionFactory(PgUrl pgUrl, boolean forPoolVendor) {
        this.pgUrl = pgUrl;
        this.forPoolVendor = forPoolVendor;
        this.sessionAdjutant = new PgSessionAdjutant(this);
    }


    @Override
    public Publisher<LocalDatabaseSession> localSession() {
        if (this.closed.get()) {
            return Mono.error(PgExceptions.factoryClosed(this.name));
        }
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createTxSession);
    }

    @Override
    public Publisher<RmDatabaseSession> rmSession() {
        if (this.closed.get()) {
            return Mono.error(PgExceptions.factoryClosed(this.name));
        }
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createXaSession);
    }

    @Override
    public <T> Publisher<T> close() {
        this.closed.set(true);
        return Mono.empty();
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
            session = PgRmDatabaseSession.forPoolVendor(this.sessionAdjutant, protocol);
        } else {
            session = PgRmDatabaseSession.create(this.sessionAdjutant, protocol);
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
