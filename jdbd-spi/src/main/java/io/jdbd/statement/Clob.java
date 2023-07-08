package io.jdbd.statement;

import io.jdbd.lang.NonNull;
import org.reactivestreams.Publisher;

/**
 * @see Text
 * @since 1.0
 */
public interface Clob extends Parameter {

    @NonNull
    Publisher<CharSequence> value();

    static Clob from(Publisher<CharSequence> source) {
        return JdbdParameters.clobParam(source);
    }

}
