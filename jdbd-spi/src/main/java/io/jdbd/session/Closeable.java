package io.jdbd.session;

import io.jdbd.JdbdException;
import org.reactivestreams.Publisher;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link DatabaseSessionFactory}</li>
 *         <li>{@link DatabaseSession}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface Closeable {

    /**
     * @return the {@link Publisher} that emit nothing or emit {@link JdbdException}
     */
    <T> Publisher<T> close();


}
