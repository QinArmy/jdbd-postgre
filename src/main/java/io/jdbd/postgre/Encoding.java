package io.jdbd.postgre;


import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public abstract class Encoding {

    protected Encoding() {
        throw new UnsupportedOperationException();
    }

    // private static final Map<String, List<String>>

    public static final Charset CLIENT_CHARSET = StandardCharsets.UTF_8;

}
