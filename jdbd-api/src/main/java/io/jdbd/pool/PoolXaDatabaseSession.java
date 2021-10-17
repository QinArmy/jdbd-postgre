package io.jdbd.pool;

import io.jdbd.session.XaDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolXaDatabaseSession extends XaDatabaseSession, PoolDatabaseSession {


    @Override
    Publisher<PoolXaDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    @Override
    Publisher<PoolXaDatabaseSession> reset();



}
