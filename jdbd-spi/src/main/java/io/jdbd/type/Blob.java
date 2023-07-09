package io.jdbd.type;


import io.jdbd.lang.NonNull;
import org.reactivestreams.Publisher;


public interface Blob extends PublisherParameter {

    @NonNull
    Publisher<byte[]> value();

    static Blob from(Publisher<byte[]> source) {
        return JdbdTypes.blobParam(source);
    }

}
