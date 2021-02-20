package io.jdbd;

public final class ErrorSubscribeException extends JdbdNonSQLException {

    public static ErrorSubscribeException expectQuery() {
        throw new UnsupportedOperationException();
    }

    public static ErrorSubscribeException expectUpdate() {
        throw new UnsupportedOperationException();
    }

    public static ErrorSubscribeException expectBatchUpdate() {
        throw new UnsupportedOperationException();
    }


    public static ErrorSubscribeException errorSubscribe(SubscriptionType expect, SubscriptionType actual
            , String format, Object... args) {
        return new ErrorSubscribeException(expect, actual, format, args);
    }

    private final SubscriptionType expectedType;

    private final SubscriptionType actualType;

    private ErrorSubscribeException(SubscriptionType expectedType, SubscriptionType actualType
            , String format, Object... args) {
        super(createMessage(format, args));
        this.expectedType = expectedType;
        this.actualType = actualType;
    }

    public SubscriptionType getActualType() {
        return this.actualType;
    }

    public enum SubscriptionType {
        UPDATE,
        QUERY,
        BATCH_UPDATE
    }
}
