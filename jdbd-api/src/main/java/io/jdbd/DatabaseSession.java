package io.jdbd;

import io.jdbd.meta.DatabaseMetaData;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import org.reactivestreams.Publisher;

import java.sql.Connection;
import java.sql.Savepoint;

public interface DatabaseSession extends ReactiveCloseable {

    DatabaseMetaData getDatabaseMetaData();


    /**
     * @see Connection#createStatement()
     */
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

    /**
     * @see java.sql.Connection#setSavepoint()
     */
    Publisher<Savepoint> setSavepoint();

    /**
     * @see java.sql.Connection#setSavepoint(String)
     */
    Publisher<Savepoint> setSavepoint(String name);

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
