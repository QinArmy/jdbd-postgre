package io.jdbd;

public final class NotFoundDriverException extends JdbdNonSQLException {

    private final String url;

    public NotFoundDriverException(String url) {
        super("Not found the driver that can accept url,please check classpath for target driver.");
        this.url = url;
    }


    public final String getUrl() {
        return this.url;
    }


}
