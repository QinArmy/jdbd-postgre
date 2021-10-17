package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.session.SessionAdjutant;
import reactor.core.publisher.Mono;

public abstract class ClientProtocolFactory {

    private ClientProtocolFactory() {
        throw new UnsupportedOperationException();
    }


    public static Mono<ClientProtocol> single(SessionAdjutant sessionAdjutant) {
        return MySQLTaskExecutor.create(0, sessionAdjutant)//1. create tcp connection
                .map(executor -> new SessionManagerImpl(executor, sessionAdjutant, 0))//2. create  SessionManagerImpl
                .flatMap(SessionManagerImpl::authenticate) //3. authenticate
                .flatMap(SessionManagerImpl::initializing)//4. initializing
                .flatMap(SessionManager::reset)           //5. reset
                .map(ClientProtocolImpl::create);         //6. create ClientProtocol
    }


    private static final class SessionManagerImpl implements SessionManager {

        private final MySQLTaskExecutor executor;

        private final SessionAdjutant sessionAdjutant;

        private final int hostIndex;

        private SessionManagerImpl(MySQLTaskExecutor executor, SessionAdjutant sessionAdjutant, int hostIndex) {
            this.executor = executor;
            this.sessionAdjutant = sessionAdjutant;
            this.hostIndex = hostIndex;
        }

        private Mono<SessionManagerImpl> authenticate() {
            return MySQLConnectionTask.authenticate(this.executor.taskAdjutant())
                    .then(Mono.empty())
                    ;
        }

        private Mono<SessionManagerImpl> initializing() {
            return Mono.empty();
        }

        @Override
        public TaskAdjutant adjutant() {
            return this.executor.taskAdjutant();
        }

        @Override
        public Mono<SessionManager> reset() {
            return null;
        }

        @Override
        public Mono<Void> reConnect() {
            return null;
        }

    }


}
