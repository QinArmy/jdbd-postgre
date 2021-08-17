package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Server;
import io.jdbd.postgre.session.SessionAdjutant;
import reactor.core.publisher.Mono;

public abstract class ClientProtocolFactory {

    private ClientProtocolFactory() {
        throw new UnsupportedOperationException();
    }

    public static Mono<ClientProtocol> single(SessionAdjutant sessionAdjutant, int hostIndex) {
        return PgTaskExecutor.create(sessionAdjutant, hostIndex)
                .map(executor -> new ConnectionManagerImpl(executor, sessionAdjutant, hostIndex))
                .flatMap(ConnectionManagerImpl::connect)
                .map(ClientProtocolImpl::create);
    }


    private static final class ConnectionManagerImpl implements ConnectionManager {

        final PgTaskExecutor executor;

        final SessionAdjutant sessionAdjutant;

        final int hostIndex;

        private ConnectionManagerImpl(PgTaskExecutor executor, SessionAdjutant sessionAdjutant, int hostIndex) {
            this.executor = executor;
            this.sessionAdjutant = sessionAdjutant;
            this.hostIndex = hostIndex;
        }

        @Override
        public final Mono<Void> reConnect() {
            return Mono.empty();
        }

        @Override
        public final Mono<Server> reset() {
            return Mono.empty();
        }

        @Override
        public final TaskAdjutant taskAdjutant() {
            return this.executor.taskAdjutant();
        }

        /*################################## blow private method ##################################*/

        private Mono<ConnectionManagerImpl> connect() {
            return authenticateAndInitializing()
                    .thenReturn(this);
        }

        private Mono<Void> authenticateAndInitializing() {
            return PgConnectionTask.authenticate(this.executor.taskAdjutant())
                    .doOnSuccess(result -> PgTaskExecutor.handleAuthenticationSuccess(this.executor, result))
                    .then(Mono.defer(this::initializing));

        }

        private Mono<Void> initializing() {
            return Mono.empty();
        }

    }


}
