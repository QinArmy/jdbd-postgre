package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.protocol.MySQLProtocolFactory;
import io.jdbd.mysql.protocol.ProtocolFactories;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.RmDatabaseSession;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Map;

public final class MySQLDatabaseSessionFactory implements DatabaseSessionFactory {

    static final String MY_SQL = "MySQL";

    public static MySQLDatabaseSessionFactory create(String url, Map<String, Object> properties)
            throws JdbdException {
        return new MySQLDatabaseSessionFactory(ProtocolFactories.from(url, properties), false);
    }

    public static MySQLDatabaseSessionFactory forPoolVendor(String url, Map<String, Object> properties)
            throws JdbdException {
        return new MySQLDatabaseSessionFactory(ProtocolFactories.from(url, properties), true);
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





}
