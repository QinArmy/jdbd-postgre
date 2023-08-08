package io.jdbd.session;

import io.jdbd.JdbdException;
import org.reactivestreams.Publisher;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link DatabaseSessionFactory}</li>
 *         <li>{@link DatabaseSession}</li>
 *         <li>{@link io.jdbd.result.RefCursor}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface Closeable {

    /**
     * <p>
     * Close underlying resource.
     * </p>
     *
     * @param <T> representing any java type,because this method usually is used with concatWith(Publisher) method.
     * @return the {@link Publisher} that emit nothing or emit {@link JdbdException}
     */
    <T> Publisher<T> close();


}
