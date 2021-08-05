package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Server;
import io.jdbd.postgre.session.SessionAdjutant;
import reactor.core.publisher.Mono;

public abstract class ClientProtocolFactory {

    private ClientProtocolFactory() {
        throw new UnsupportedOperationException();
    }


    public static Mono<ClientProtocol> create(SessionAdjutant sessionAdjutant, int hostIndex) {
        return PostgreTaskExecutor.create(sessionAdjutant, hostIndex)
                .flatMap(executor -> new ConnectionManagerImpl(executor, sessionAdjutant, hostIndex)
                        .authenticateAndInitializing()
                );
    }


    static final class ConnectionManagerImpl implements ConnectionManager {

        final PostgreTaskExecutor executor;

        final TaskAdjutant adjutant;

        final SessionAdjutant sessionAdjutant;

        final int hostIndex;

        private ConnectionManagerImpl(PostgreTaskExecutor executor, SessionAdjutant sessionAdjutant, int hostIndex) {
            this.executor = executor;
            this.adjutant = executor.getAdjutant();
            this.sessionAdjutant = sessionAdjutant;
            this.hostIndex = hostIndex;
        }


        @Override
        public final Mono<Void> connect() {
            return null;
        }

        @Override
        public final Mono<Void> reConnect() {
            return null;
        }

        @Override
        public final Mono<Server> reset() {
            return null;
        }

        private Mono<ClientProtocol> authenticateAndInitializing() {
            /*return PostgreConnectionTask.authenticate(this.adjutant)
                    .doOnSuccess(authResult -> {})
                    .map(ClientProtocolImpl.create(this))*/

            return Mono.empty();

        }

    }


}
