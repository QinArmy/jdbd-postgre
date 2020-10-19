package io.jdbd;

import io.jdbd.meta.DatabaseMetaData;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.Savepoint;

public interface StatelessSession extends ReactiveCloseable{

    DatabaseMetaData getDatabaseMetaData();

    /**
     * @see Connection#isReadOnly()
     * @see Connection#getTransactionIsolation()
     * @see Connection#getAutoCommit()
     */
    Mono<? extends TransactionOption> getTransactionOption();

    /**
     * @see Connection#createStatement()
     */
    Mono<Statement> createStatement();

    /**
     * @see java.sql.Connection#prepareStatement(String)
     */
    Mono<PreparedStatement> prepareStatement(String sql);

    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    boolean supportSavePoints();

    /**
     * @see java.sql.Connection#setSavepoint()
     */
    Mono<? extends Savepoint> setSavepoint();

    /**
     * @see java.sql.Connection#setSavepoint(String)
     */
    Mono<? extends Savepoint> setSavepoint(String name);

    /**
     * @see java.sql.Connection#releaseSavepoint(Savepoint)
     */
    Mono<Void> releaseSavePoint(Savepoint savepoint);

    /**
     * @see java.sql.Connection#rollback(Savepoint)
     */
    Mono<Void> rollbackToSavePoint(Savepoint savepoint);


    /**
     * @see Connection#isClosed()
     */
    Mono<Boolean> isClosed();


}
