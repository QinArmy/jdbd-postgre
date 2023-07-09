package io.jdbd.type;

import io.jdbd.lang.NonNull;
import org.reactivestreams.Publisher;

import java.nio.charset.Charset;

public interface Text extends PublisherParameter {

    Charset charset();

    @NonNull
    Publisher<byte[]> value();


    static Text from(Charset charset, Publisher<byte[]> source) {
        return JdbdTypes.textParam(charset, source);
    }

}
