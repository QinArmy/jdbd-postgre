package io.jdbd.pool;

import io.jdbd.session.DatabaseSession;
import org.reactivestreams.Publisher;

/**
 * <p>
 * This interface representing {@link DatabaseSession} than can be pooled.
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link PoolLocalDatabaseSession}</li>
 *         <li>{@link PoolGlobalDatabaseSession}</li>
 *     </ul>
 * </p>
 * <p>
 *     This interface is used by pool vendor,application developer shouldn't use this interface.
 *     Driver developer create the instance of this interface,but driver developer don't use this interface,
 *     because driver developer are not responsible for pooling.
 *
 * </p>
 *
 * @since 1.0
 */
public interface PoolDatabaseSession extends DatabaseSession {

    Publisher<? extends PoolDatabaseSession> reconnect(int maxReconnect);

    Publisher<? extends PoolDatabaseSession> ping(int timeoutSeconds);


    /**
     * @return Publisher that emit this when success.
     */
    Publisher<? extends PoolDatabaseSession> reset();


}
