package io.jdbd;


import io.jdbd.lang.Nullable;


public class JdbdException extends RuntimeException {

    private final String sqlState;

    private final int vendorCode;

    public JdbdException(String message) {
        super(message);
        this.sqlState = null;
        this.vendorCode = 0;
    }

    public JdbdException(String message, String sqlState, int vendorCode) {
        super(message);
        this.sqlState = sqlState;
        this.vendorCode = vendorCode;
    }

    public JdbdException(String message, @Nullable Throwable cause) {
        super(message, cause);
        this.sqlState = null;
        this.vendorCode = 0;
    }

    @Deprecated
    JdbdException(@Nullable Throwable cause, String message) {
        super(message, cause);
        this.sqlState = null;
        this.vendorCode = 0;
    }

    public JdbdException(String message, @Nullable Throwable cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.sqlState = null;
        this.vendorCode = 0;
    }


    protected static String createMessage(@Nullable String messageFormat, @Nullable Object... args) {
        String msg;
        if (messageFormat != null && args != null && args.length > 0) {
            msg = String.format(messageFormat, args);
        } else {
            msg = messageFormat;
        }
        return msg;
    }


}
