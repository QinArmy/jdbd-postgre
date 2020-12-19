package io.jdbd;

public final class SubscriptionNotMatchException extends JdbdNonSQLException {

    public static SubscriptionNotMatchException expectQuery() {
        return new SubscriptionNotMatchException(SubscriptionType.QUERY, "expect subscribe nextQuery(),but not.");
    }

    public static SubscriptionNotMatchException expectUpdate() {
        return new SubscriptionNotMatchException(SubscriptionType.UPDATE, "expect subscribe nextUpdate(),but not.");
    }

    public static SubscriptionNotMatchException expectBatchUpdate() {
        return new SubscriptionNotMatchException(SubscriptionType.BATCH_UPDATE, "expect subscribe batchUpdate(),but not.");
    }

    private final SubscriptionType expectedType;

    private SubscriptionNotMatchException(SubscriptionType expectedType, String message) {
        super(message);
        this.expectedType = expectedType;
    }

    public SubscriptionType getExpectedType() {
        return this.expectedType;
    }

    public enum SubscriptionType {
        UPDATE,
        QUERY,
        BATCH_UPDATE
    }
}
