package io.jdbd;

import reactor.core.publisher.Mono;

public interface DatabaseSession  extends StatelessSession {


    Mono<Void> startTransaction(TransactionOption option);

    Mono<Void> commit();

    Mono<Void> rollback();

}
