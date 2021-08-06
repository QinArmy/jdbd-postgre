package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PostgreUrl;
import io.jdbd.postgre.session.SessionAdjutant;
import io.netty.channel.EventLoopGroup;
import org.testng.Assert;
import reactor.netty.resources.LoopResources;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

class AbstractTaskTests {


    static final Queue<ClientProtocolImpl> PROTOCOL_QUEUE = new LinkedBlockingQueue<>();

    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-postgre", 20, true)
            .onClient(true);

    private static final SessionAdjutant DEFAULT_SESSION_ADJUTANT = createDefaultSessionAdjutant();


    static ClientProtocolImpl obtainProtocol() {
        ClientProtocolImpl protocol = PROTOCOL_QUEUE.poll();
        if (protocol == null) {
            protocol = ClientProtocols.single(DEFAULT_SESSION_ADJUTANT, 0)
                    .cast(ClientProtocolImpl.class)
                    .block();
            Assert.assertNotNull(protocol, "protocol");
        } else {
            protocol.reset()
                    .block();
        }
        return protocol;
    }

    static void releaseConnection(ClientProtocolImpl protocol) {
        PROTOCOL_QUEUE.offer(protocol);
    }

    static SessionAdjutant createDefaultSessionAdjutant() {
        Map<String, String> propMap = new HashMap<>();
        return new SessionAdjutantImpl(ClientTestUtils.createUrl(propMap));
    }


    private static final class SessionAdjutantImpl implements SessionAdjutant {

        private final PostgreUrl postgreUrl;

        private SessionAdjutantImpl(PostgreUrl postgreUrl) {
            this.postgreUrl = postgreUrl;
        }

        @Override
        public PostgreUrl obtainUrl() {
            return this.postgreUrl;
        }

        @Override
        public EventLoopGroup obtainEventLoopGroup() {
            return EVENT_LOOP_GROUP;
        }

    }


}
