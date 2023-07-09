package io.jdbd.type;

import io.jdbd.lang.NonNull;
import io.jdbd.statement.Parameter;
import org.reactivestreams.Publisher;

/**
 * @see Text
 * @since 1.0
 */
public interface Clob extends Parameter {

    @NonNull
    Publisher<CharSequence> value();

    static Clob from(Publisher<CharSequence> source) {
        return JdbdTypes.clobParam(source);
    }

}
