package io.jdbd;

import reactor.core.publisher.Mono;

public interface DatabaseSession  extends StatelessDatabaseSession{


    Mono<Void> startTransaction(TransactionOption option);

    Statement createStatement();



}
