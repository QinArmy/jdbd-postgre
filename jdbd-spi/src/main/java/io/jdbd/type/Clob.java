package io.jdbd.type;

import io.jdbd.lang.NonNull;
import org.reactivestreams.Publisher;

/**
 * @see Text
 * @since 1.0
 */
public interface Clob extends PublisherParameter {

    @NonNull
    Publisher<CharSequence> value();

    static Clob from(Publisher<CharSequence> source) {
        return JdbdTypes.clobParam(source);
    }

}
