package io.jdbd.statement;

import io.jdbd.lang.NonNull;
import org.reactivestreams.Publisher;

import java.nio.charset.Charset;

public interface Text extends Parameter {

    Charset charset();

    @NonNull
    Publisher<byte[]> value();


    static Text from(Charset charset, Publisher<byte[]> source) {
        return JdbdParameters.textParam(charset, source);
    }

}
