package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.ClientTestUtils;
import io.jdbd.postgre.config.PgKey;
import io.jdbd.postgre.config.PgUrl;
import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.JdbdCompositeException;
import io.netty.channel.EventLoopGroup;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

abstract class AbstractTaskTests {


    static final Queue<ClientProtocol> PROTOCOL_QUEUE = new LinkedBlockingQueue<>();

    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-postgre", 20, true)
            .onClient(true);

    static final SessionAdjutant DEFAULT_SESSION_ADJUTANT = createDefaultSessionAdjutant();

    static final int UNKNOWN_SCALE = 0;

    static final long UNKNOWN_PRECISION = 0;


    static ResultStates assertUpdateOne(ResultStates state) {
        assertEquals(state.getAffectedRows(), 1L, "affectedRows");
        return state;
    }


    static Mono<ClientProtocol> obtainProtocol() {
        final ClientProtocol protocol = PROTOCOL_QUEUE.poll();
        final Mono<ClientProtocol> mono;
        if (protocol == null) {
            mono = ClientProtocolFactory.single(DEFAULT_SESSION_ADJUTANT, 0);
        } else {
            mono = protocol.reset();
        }
        return mono;
    }

    static ClientProtocol obtainProtocolWithSync() {
        ClientProtocol protocol;
        protocol = obtainProtocol()
                .block();
        assertNotNull(protocol, "protocol");
        return protocol;
    }

    static TaskAdjutant mapToTaskAdjutant(ClientProtocol protocol) {
        return ((ClientProtocolImpl) protocol).adjutant;
    }

    static <T> Mono<T> releaseConnection(ClientProtocol protocol) {
        return protocol.reset()
                .doOnSuccess(v -> PROTOCOL_QUEUE.offer(protocol))
                .then(Mono.empty());
    }

    static <T> Function<? super Throwable, ? extends Mono<T>> releaseConnectionOnError(ClientProtocol protocol) {
        return error -> protocol.reset()
                .onErrorMap(resetError -> {
                    List<Throwable> list = new ArrayList<>(2);
                    list.add(error);
                    list.add(resetError);
                    return new JdbdCompositeException(list);
                }).then(Mono.error(error));
    }


    static SessionAdjutant createDefaultSessionAdjutant() {
        Map<String, String> propMap = new HashMap<>();
        propMap.put(PgKey.lc_monetary.getKey(), getDefaultLcMonetary() + ".UTF-8");
        return new SessionAdjutantImpl(ClientTestUtils.createUrl(propMap));
    }

    static Locale getDefaultLcMonetary() {
        return Locale.CHINA;
    }


    private static final class SessionAdjutantImpl implements SessionAdjutant {

        private final PgUrl pgUrl;

        private SessionAdjutantImpl(PgUrl pgUrl) {
            this.pgUrl = pgUrl;
        }

        @Override
        public PgUrl jdbcUrl() {
            return this.pgUrl;
        }

        @Override
        public EventLoopGroup eventLoopGroup() {
            return EVENT_LOOP_GROUP;
        }

        @Override
        public boolean isSameFactory(DatabaseSessionFactory factory) {
            return false;
        }
    }


}
