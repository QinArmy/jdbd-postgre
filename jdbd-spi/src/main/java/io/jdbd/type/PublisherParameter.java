package io.jdbd.type;

import io.jdbd.lang.NonNull;
import org.reactivestreams.Publisher;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link Blob}</li>
 *         <li>{@link Clob}</li>
 *         <li>{@link Text}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface PublisherParameter extends LongParameter {

    @NonNull
    Publisher<?> value();


}
