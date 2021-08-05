package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.session.SessionAdjutant;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.stmt.Stmt;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

final class ClientProtocolImpl implements ClientProtocol {

    static ClientProtocolImpl create(ClientProtocolFactory.ConnectionManagerImpl builder) {
        return new ClientProtocolImpl(builder);
    }

    private final PostgreTaskExecutor executor;

    private final TaskAdjutant adjutant;

    private final SessionAdjutant sessionAdjutant;

    private final int hostIndex;


    private ClientProtocolImpl(ClientProtocolFactory.ConnectionManagerImpl builder) {
        this.executor = builder.executor;
        this.adjutant = this.executor.getAdjutant();
        this.sessionAdjutant = builder.sessionAdjutant;
        this.hostIndex = builder.hostIndex;

    }

    @Override
    public Mono<ResultState> update(Stmt stmt) {
        return null;
    }

    @Override
    public Flux<ResultRow> query(Stmt stmt) {
        return null;
    }

    @Override
    public Mono<Void> close() {
        return null;
    }


}
