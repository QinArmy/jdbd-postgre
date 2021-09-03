package io.jdbd.pool;

import org.reactivestreams.Publisher;

public interface PoolTxDatabaseSession extends PoolDatabaseSession {

    @Override
    Publisher<PoolTxDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    Publisher<PoolTxDatabaseSession> reset();


}
