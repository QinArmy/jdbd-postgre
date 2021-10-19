package io.jdbd.stmt;

import io.jdbd.JdbdNonSQLException;

public final class SubscribeException extends JdbdNonSQLException {

    @Deprecated
    public static SubscribeException expectQuery() {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static SubscribeException expectUpdate() {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static SubscribeException expectBatchUpdate() {
        throw new UnsupportedOperationException();
    }


    @Deprecated
    public static SubscribeException errorSubscribe(ResultType expect, ResultType actual
            , String format, Object... args) {
        return new SubscribeException(expect, actual, format);
    }

    private final ResultType subscribeType;

    private final ResultType actualType;

    public SubscribeException(ResultType subscribeType, ResultType actualType) {
        super("Subscribe ResultType[%s] but actual ResultType[%s]", subscribeType, actualType);
        this.subscribeType = subscribeType;
        this.actualType = actualType;
    }

    public SubscribeException(ResultType subscribeType, ResultType actualType
            , String message) {
        super(message);
        this.subscribeType = subscribeType;
        this.actualType = actualType;
    }

    public ResultType getActualType() {
        return this.actualType;
    }

    public ResultType getSubscribeType() {
        return this.subscribeType;
    }
}
