package io.jdbd.pool;

import io.jdbd.session.DatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolDatabaseSession extends DatabaseSession {

    Publisher<? extends PoolDatabaseSession> ping(int timeoutSeconds);


    /**
     * @return Publisher that emit this when success.
     */
    Publisher<? extends PoolDatabaseSession> reset();


}
