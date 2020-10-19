package io.jdbd;

public class UrlException extends JdbdRuntimeException{

    private final String url;

    public UrlException(String message, String url) {
        super(message);
        this.url = url;
    }

    public UrlException(String message, Throwable cause, String url) {
        super(message, cause);
        this.url = url;
    }

    public UrlException(String message, Throwable cause
            , boolean enableSuppression, boolean writableStackTrace, String url) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.url = url;
    }

    public String getUrl() {
        return this.url;
    }
}
