package io.jdbd.type;

import io.jdbd.lang.NonNull;
import io.jdbd.statement.Parameter;
import org.reactivestreams.Publisher;

import java.nio.charset.Charset;

public interface Text extends Parameter {

    Charset charset();

    @NonNull
    Publisher<byte[]> value();


    static Text from(Charset charset, Publisher<byte[]> source) {
        return JdbdTypes.textParam(charset, source);
    }

}
