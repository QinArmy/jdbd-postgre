package io.jdbd.session;

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

    Publisher<Void> close();


}
