package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.config.PostgreUrl;
import io.jdbd.postgre.session.SessionAdjutant;
import io.netty.channel.EventLoopGroup;
import reactor.netty.resources.LoopResources;

import java.util.HashMap;
import java.util.Map;

class AbstractTaskTests {

    private final static EventLoopGroup EVENT_LOOP_GROUP = LoopResources.create("jdbd-postgre", 20, true)
            .onClient(true);


    SessionAdjutant createDefaultSessionAdjutant() {
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
