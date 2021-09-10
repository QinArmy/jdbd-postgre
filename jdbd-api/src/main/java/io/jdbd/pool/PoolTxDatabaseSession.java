package io.jdbd.pool;

import io.jdbd.TxDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolTxDatabaseSession extends PoolDatabaseSession, TxDatabaseSession {

    @Override
    Publisher<PoolTxDatabaseSession> ping(int timeoutSeconds);

    /**
     * @return Publisher that emit this when success.
     */
    Publisher<PoolTxDatabaseSession> reset();


}
