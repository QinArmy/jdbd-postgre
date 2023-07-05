package io.jdbd.pool;

import io.jdbd.session.LocalDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolTxDatabaseSession extends PoolDatabaseSession, LocalDatabaseSession {


    @Override
    Publisher<PoolTxDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    Publisher<PoolTxDatabaseSession> reset();


}
