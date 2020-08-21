package io.jdbd;

import io.jdbd.meta.DatabaseMetaData;
import reactor.core.publisher.Mono;

public interface StatelessDatabaseSession {

    DatabaseMetaData getDatabaseMetaData();

    Mono<? extends TransactionOption> getTransactionOption();

}
