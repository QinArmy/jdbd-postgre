package io.jdbd;

import io.jdbd.meta.DatabaseMetaData;
import org.reactivestreams.Publisher;

import java.sql.Connection;
import java.sql.Savepoint;

public interface StatelessSession extends ReactiveCloseable{

    DatabaseMetaData getDatabaseMetaData();

    /**
     * @see Connection#isReadOnly()
     * @see Connection#getTransactionIsolation()
     * @see Connection#getAutoCommit()
     */
    Publisher<? extends TransactionOption> getTransactionOption();

    /**
     * @see Connection#createStatement()
     */
    Publisher<StaticStatement> createStatement();

    /**
     * @see java.sql.Connection#prepareStatement(String)
     */
    Publisher<PreparedStatement> prepareStatement(String sql);

    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    boolean supportSavePoints();

    /**
     * @see java.sql.Connection#setSavepoint()
     */
    Publisher<? extends Savepoint> setSavepoint();

    /**
     * @see java.sql.Connection#setSavepoint(String)
     */
    Publisher<? extends Savepoint> setSavepoint(String name);

    /**
     * @see java.sql.Connection#releaseSavepoint(Savepoint)
     */
    Publisher<Void> releaseSavePoint(Savepoint savepoint);

    /**
     * @see java.sql.Connection#rollback(Savepoint)
     */
    Publisher<Void> rollbackToSavePoint(Savepoint savepoint);


    /**
     * @see Connection#isClosed()
     */
    Publisher<Boolean> isClosed();


}
