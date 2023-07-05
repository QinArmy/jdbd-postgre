package io.jdbd.pool;

import io.jdbd.session.RmDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolGlobalDatabaseSession extends RmDatabaseSession, PoolDatabaseSession {


    @Override
    Publisher<PoolGlobalDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    @Override
    Publisher<PoolGlobalDatabaseSession> reset();


}
