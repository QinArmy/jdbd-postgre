package io.jdbd.pool;

import io.jdbd.session.LocalDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolLocalDatabaseSession extends PoolDatabaseSession, LocalDatabaseSession {


    @Override
    Publisher<PoolLocalDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    Publisher<PoolLocalDatabaseSession> reset();


}
