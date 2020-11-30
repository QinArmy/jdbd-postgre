package io.jdbd;

public final class SubscriptionNotMatchException extends JdbdNonSQLException {

    public static SubscriptionNotMatchException expectQuery() {
        return new SubscriptionNotMatchException(SubscriptionType.QUERY, "expect subscribe nextQuery(),but nextUpdate.");
    }

    public static SubscriptionNotMatchException expectUpdate() {
        return new SubscriptionNotMatchException(SubscriptionType.UPDATE, "expect subscribe nextUpdate(),but nextQuery.");
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
        QUERY
    }
}
