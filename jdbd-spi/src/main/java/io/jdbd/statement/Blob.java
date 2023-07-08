package io.jdbd.statement;


import io.jdbd.lang.NonNull;
import org.reactivestreams.Publisher;


public interface Blob extends Parameter {

    @NonNull
    Publisher<byte[]> value();

    static Blob from(Publisher<byte[]> source) {
        return JdbdParameters.blobParam(source);
    }

}
