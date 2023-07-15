package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.mysql.env.Environment;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.client.AuthenticationPlugin;
import io.jdbd.mysql.protocol.client.ClientProtocolFactory;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
import io.netty.channel.EventLoopGroup;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
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
        return ClientProtocolFactory.from(Environment.parse(url, properties));
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
            throw new UnsupportedOperationException();
        }

        @Override
        public MySQLUrl jdbcUrl() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Class<? extends AuthenticationPlugin>> pluginClassMap() {
            throw new UnsupportedOperationException();
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




}
