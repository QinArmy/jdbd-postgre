package io.jdbd.pool;

import io.jdbd.session.RmDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolRmDatabaseSession extends RmDatabaseSession, PoolDatabaseSession {

    /**
     * @return Publisher that emit this when success.
     */
    @Override
    Publisher<PoolRmDatabaseSession> reconnect();

    /**
     * @return Publisher that emit this when success.
     */
    @Override
    Publisher<PoolRmDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    @Override
    Publisher<PoolRmDatabaseSession> reset();


}
