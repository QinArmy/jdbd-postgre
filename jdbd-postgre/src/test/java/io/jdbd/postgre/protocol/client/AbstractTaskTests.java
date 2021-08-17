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


    static final Queue<ClientProtocol> PROTOCOL_QUEUE = new LinkedBlockingQueue<>();

    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-postgre", 20, true)
            .onClient(true);

    private static final SessionAdjutant DEFAULT_SESSION_ADJUTANT = createDefaultSessionAdjutant();


    static ClientProtocol obtainProtocol() {
        ClientProtocol protocol = PROTOCOL_QUEUE.poll();
        if (protocol == null) {
            protocol = ClientProtocolFactory.single(DEFAULT_SESSION_ADJUTANT, 0)
                    .block();
            Assert.assertNotNull(protocol, "protocol");
        } else {
            protocol.reset()
                    .block();
        }
        return protocol;
    }

    static TaskAdjutant obtainTaskAdjutant(ClientProtocol protocol) {
        return ((ClientProtocolImpl) protocol).adjutant;
    }

    static void releaseConnection(ClientProtocol protocol) {
        protocol.reset()
                .doAfterTerminate(() -> PROTOCOL_QUEUE.offer(protocol))
                .subscribe();

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
