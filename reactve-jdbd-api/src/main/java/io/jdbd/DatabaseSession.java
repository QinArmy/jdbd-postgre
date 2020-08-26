package io.jdbd;

import reactor.core.publisher.Mono;

public interface DatabaseSession  extends StatelessDatabaseSession{


    Mono<Void> startTransaction(TransactionOption option);

    Mono<Void> commit();

    Mono<Void> rollback();

}
