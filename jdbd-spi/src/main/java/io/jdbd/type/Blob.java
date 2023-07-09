package io.jdbd.type;


import io.jdbd.lang.NonNull;
import io.jdbd.statement.Parameter;
import org.reactivestreams.Publisher;


public interface Blob extends Parameter {

    @NonNull
    Publisher<byte[]> value();

    static Blob from(Publisher<byte[]> source) {
        return JdbdTypes.blobParam(source);
    }

}
