package io.jdbd.pool;

import io.jdbd.session.RmDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolXaDatabaseSession extends RmDatabaseSession, PoolDatabaseSession {


    @Override
    Publisher<PoolXaDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    @Override
    Publisher<PoolXaDatabaseSession> reset();


}
