package io.jdbd.postgre.protocol;

import io.jdbd.postgre.protocol.client.PgProtocol;
import reactor.core.publisher.Mono;


/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link io.jdbd.postgre.protocol.client.ClientProtocolFactory}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface PgProtocolFactory {

    String factoryName();

    Mono<PgProtocol> createProtocol();


    /**
     * override {@link Object#toString()}
     *
     * @return driver info, contain : <ol>
     * <li>implementation class name</li>
     * <li>{@link #factoryName()}</li>
     * <li>{@link System#identityHashCode(Object)}</li>
     * </ol>
     */
    @Override
    String toString();


}
