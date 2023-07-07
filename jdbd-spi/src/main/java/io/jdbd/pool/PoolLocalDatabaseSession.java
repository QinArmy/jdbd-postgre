package io.jdbd.pool;

import io.jdbd.session.LocalDatabaseSession;
import org.reactivestreams.Publisher;

public interface PoolLocalDatabaseSession extends PoolDatabaseSession, LocalDatabaseSession {

    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<PoolLocalDatabaseSession> reconnect(int maxReconnect);

    /**
     * {@inheritDoc}
     */
    @Override
    Publisher<PoolLocalDatabaseSession> ping(int timeoutSeconds);

    /**
     * {@inheritDoc}
     */
    Publisher<PoolLocalDatabaseSession> reset();


}
