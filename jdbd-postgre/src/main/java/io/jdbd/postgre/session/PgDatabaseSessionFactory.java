package io.jdbd.postgre.session;

import io.jdbd.DatabaseSessionFactory;
import io.jdbd.TxDatabaseSession;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.config.PgKey;
import io.jdbd.postgre.config.PostgreUrl;
import io.jdbd.postgre.protocol.client.ClientProtocol;
import io.jdbd.postgre.protocol.client.ClientProtocolFactory;
import io.jdbd.xa.XaDatabaseSession;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class PgDatabaseSessionFactory implements DatabaseSessionFactory {

    /**
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    public static PgDatabaseSessionFactory create(final String url, final Map<String, String> properties) {
        final PostgreUrl postgreUrl;
        postgreUrl = PostgreUrl.create(url, properties);
        return new PgDatabaseSessionFactory(properties, postgreUrl, false);
    }

    /**
     * @throws io.jdbd.config.UrlException      when url error.
     * @throws io.jdbd.config.PropertyException when properties error.
     */
    public static PgDatabaseSessionFactory forPoolVendor(final String url, final Map<String, String> properties) {
        final PostgreUrl postgreUrl;
        postgreUrl = PostgreUrl.create(url, properties);
        return new PgDatabaseSessionFactory(properties, postgreUrl, true);
    }


    public static boolean acceptsUrl(String url) {
        return PostgreUrl.acceptsUrl(url);
    }


    private final Map<String, String> properties;

    private final PostgreUrl postgreUrl;

    private final boolean forPoolVendor;

    private final PgSessionAdjutant sessionAdjutant;


    private PgDatabaseSessionFactory(Map<String, String> properties, PostgreUrl postgreUrl, boolean forPoolVendor) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
        this.postgreUrl = postgreUrl;
        this.forPoolVendor = forPoolVendor;
        this.sessionAdjutant = new PgSessionAdjutant(this);
    }


    @Override
    public final Mono<TxDatabaseSession> getTxSession() {
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createTxSession);
    }

    @Override
    public final Mono<XaDatabaseSession> getXaSession() {
        // TODO complete me
        return ClientProtocolFactory.single(this.sessionAdjutant, 0)
                .map(this::createXaSession);
    }

    @Override
    public final String getUrl() {
        return this.postgreUrl.getOriginalUrl();
    }

    @Override
    public final Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public final int getMajorVersion() {
        final String version = PgDriver.class.getPackage().getSpecificationVersion();
        try {
            return Integer.parseInt(version.substring(0, getMajorVersionIndex(version)));
        } catch (NumberFormatException e) {
            throw createPacketWithErrorWay();
        }

    }

    @Override
    public final int getMinorVersion() {
        final String version = PgDriver.class.getPackage().getSpecificationVersion();
        if (version == null) {
            // here run in jdbd-postgre project test environment.
            throw createPacketWithErrorWay();
        }
        final int majorIndex = getMajorVersionIndex(version);
        final int minorIndex = version.indexOf('-', majorIndex);
        if (minorIndex < 0) {
            throw createPacketWithErrorWay();
        }
        try {
            return Integer.parseInt(version.substring(majorIndex + 1, minorIndex));
        } catch (NumberFormatException e) {
            throw createPacketWithErrorWay();
        }
    }

    @Override
    public final String getDriverName() {
        return PgDriver.class.getName();
    }


    /*################################## blow private method ##################################*/

    /**
     * @see #getTxSession()
     */
    private PgTxDatabaseSession createTxSession(final ClientProtocol protocol) {
        final PgTxDatabaseSession session;
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
    private PgXaDatabaseSession createXaSession(ClientProtocol protocol) {
        final PgXaDatabaseSession session;
        if (this.forPoolVendor) {
            session = PgXaDatabaseSession.forPoolVendor(this.sessionAdjutant, protocol);
        } else {
            session = PgXaDatabaseSession.create(this.sessionAdjutant, protocol);
        }
        return session;
    }


    /**
     * @see #getMajorVersion()
     * @see #getMinorVersion()
     */
    private static int getMajorVersionIndex(@Nullable String version) {

        if (version == null) {
            // here run in jdbd-postgre project test environment.
            throw createPacketWithErrorWay();
        }
        final int index = version.indexOf('.');
        if (index < 0) {
            throw createPacketWithErrorWay();
        }
        return index;
    }

    private static IllegalStateException createPacketWithErrorWay() {
        return new IllegalStateException("jdbd-postgre packet with error way.");
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
        public final PostgreUrl obtainUrl() {
            return this.factory.postgreUrl;
        }

        @Override
        public final EventLoopGroup getEventLoopGroup() {
            return this.eventLoopGroup;
        }


    }


}
