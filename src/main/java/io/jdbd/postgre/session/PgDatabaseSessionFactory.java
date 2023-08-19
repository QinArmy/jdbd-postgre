package io.jdbd.postgre.session;

import io.jdbd.DriverVersion;
import io.jdbd.JdbdException;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.env.PgHost;
import io.jdbd.postgre.env.PgUrlParser;
import io.jdbd.postgre.protocol.PgProtocolFactory;
import io.jdbd.postgre.protocol.client.ClientProtocolFactory;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.session.LocalDatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.session.RmDatabaseSession;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>
 * This class is a implementation of {@link DatabaseSessionFactory} with postgre client protocol.
 * </p>
 *
 * @since 1.0
 */
public final class PgDatabaseSessionFactory implements DatabaseSessionFactory {


    public static PgDatabaseSessionFactory create(final String url, final Map<String, Object> properties,
                                                  final boolean forPoolVendor) throws JdbdException {
        final List<PgHost> hostList;
        hostList = PgUrlParser.parse(url, properties);
        final PgProtocolFactory protocolFactory;
        protocolFactory = ClientProtocolFactory.create(hostList.get(0));
        return new PgDatabaseSessionFactory(protocolFactory, forPoolVendor);
    }


    private final PgProtocolFactory protocolFactory;

    private final String name;

    private final boolean forPoolVendor;

    private final AtomicBoolean closed = new AtomicBoolean(false);


    /**
     * <p>
     * private constructor.
     * </p>
     */
    private PgDatabaseSessionFactory(PgProtocolFactory protocolFactory, boolean forPoolVendor) {
        this.protocolFactory = protocolFactory;
        this.name = protocolFactory.factoryName();
        this.forPoolVendor = forPoolVendor;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public Publisher<LocalDatabaseSession> localSession() {
        if (this.closed.get()) {
            return Mono.error(PgExceptions.factoryClosed(this.name));
        }
        return this.protocolFactory.createProtocol()
                .map(this::createLocalSession);
    }

    @Override
    public Publisher<RmDatabaseSession> rmSession() {
        if (this.closed.get()) {
            return Mono.error(PgExceptions.factoryClosed(this.name));
        }
        return this.protocolFactory.createProtocol()
                .map(this::createRmSession);
    }

    @Override
    public <T> Publisher<T> close() {
        this.closed.set(true);
        return Mono.empty();
    }

    @Override
    public String productName() {
        return PgDriver.POSTGRE_SQL;
    }

    @Override
    public String factoryVendor() {
        return PgDriver.PG_DRIVER_VENDOR;
    }

    @Override
    public String driverVendor() {
        return PgDriver.PG_DRIVER_VENDOR;
    }

    @Override
    public DriverVersion driverVersion() {
        return PgDriver.getInstance().version();
    }

    @Override
    public <T> T valueOf(Option<T> option) {
        // currently , return null
        return null;
    }


    @Override
    public String toString() {
        return PgStrings.builder(50)
                .append(getClass().getName())
                .append("[ name : ")
                .append(name())
                .append(" , factoryVendor : ")
                .append(factoryVendor())
                .append(" , driverVendor : ")
                .append(driverVendor())
                .append(" , productName : ")
                .append(productName())
                .append(" , driverVersion : ")
                .append(driverVersion())
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }

    /*################################## blow private method ##################################*/

    /**
     * @see #localSession()
     */
    private LocalDatabaseSession createLocalSession(final PgProtocol protocol) {
        final LocalDatabaseSession session;
        if (this.forPoolVendor) {
            session = PgLocalDatabaseSession.forPoolVendor(this, protocol);
        } else {
            session = PgLocalDatabaseSession.create(this, protocol);
        }
        return session;
    }

    /**
     * @see #rmSession()
     */
    private RmDatabaseSession createRmSession(final PgProtocol protocol) {
        final RmDatabaseSession session;
        if (this.forPoolVendor) {
            session = PgRmDatabaseSession.forPoolVendor(this, protocol);
        } else {
            session = PgRmDatabaseSession.create(this, protocol);
        }
        return session;
    }


}
