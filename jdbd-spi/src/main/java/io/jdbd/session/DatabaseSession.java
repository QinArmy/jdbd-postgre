package io.jdbd.session;

import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import org.reactivestreams.Publisher;

import java.sql.Connection;


public interface DatabaseSession {

    DatabaseMetaData getDatabaseMetaData();

    /**
     * @see Connection#isReadOnly()
     * @see Connection#getTransactionIsolation()
     * @see Connection#getAutoCommit()
     */
    Publisher<TransactionOption> getTransactionOption();


    StaticStatement statement();

    /**
     * <p>
     * This method is similarly to {@code java.sql.Connection#prepareStatement(String)}
     * except that is async emit a {@link PreparedStatement}.
     * </p>
     *
     * @return A Reactive Streams {@link Publisher} with basic rx operators that completes successfully by
     * emitting an element, or with an error. Like {@code reactor.core.publisher.Mono}
     */
    Publisher<PreparedStatement> prepare(String sql);

    BindStatement bindable(String sql);

    MultiStatement multi();

    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    boolean supportSavePoints();

    boolean supportMultiStatement();


    Publisher<SavePoint> setSavePoint();


    Publisher<SavePoint> setSavePoint(String name);


    Publisher<Void> releaseSavePoint(SavePoint savepoint);


    Publisher<Void> rollbackToSavePoint(SavePoint savepoint);


    /**
     * @see Connection#isClosed()
     */
    boolean isClosed();

    ServerVersion getServerVersion();


    boolean isSameFactory(DatabaseSession session);

    boolean isBelongTo(DatabaseSessionFactory factory);

    Publisher<Void> close();

}
