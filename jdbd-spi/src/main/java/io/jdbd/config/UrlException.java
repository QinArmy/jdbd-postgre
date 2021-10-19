package io.jdbd.config;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;

public class UrlException extends JdbdNonSQLException {

    private final String url;

    public UrlException(String url, String message) {
        super(message);
        this.url = url;
    }


    public UrlException(String url, String message, @Nullable Throwable cause) {
        super(message, cause);
        this.url = url;
    }


    public final String getUrl() {
        return this.url;
    }


}
