package io.jdbd.config;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;

public class UrlException extends JdbdNonSQLException {

    private final String url;

    public UrlException(String url, String message, Object... args) {
        super(message, args);
        this.url = url;
    }

    public UrlException(@Nullable Throwable cause, String url, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
        this.url = url;
    }

    public final String getUrl() {
        return this.url;
    }


}
