package io.jdbd.stmt;

import io.jdbd.JdbdNonSQLException;

public final class ErrorSubscribeException extends JdbdNonSQLException {

    @Deprecated
    public static ErrorSubscribeException expectQuery() {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static ErrorSubscribeException expectUpdate() {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public static ErrorSubscribeException expectBatchUpdate() {
        throw new UnsupportedOperationException();
    }


    @Deprecated
    public static ErrorSubscribeException errorSubscribe(ResultType expect, ResultType actual
            , String format, Object... args) {
        return new ErrorSubscribeException(expect, actual, format, args);
    }

    private final ResultType subscribeType;

    private final ResultType actualType;

    public ErrorSubscribeException(ResultType subscribeType, ResultType actualType) {
        super("Subscribe ResultType[%s] but actual ResultType[%s]", subscribeType, actualType);
        this.subscribeType = subscribeType;
        this.actualType = actualType;
    }

    public ErrorSubscribeException(ResultType subscribeType, ResultType actualType
            , String format, Object... args) {
        super(createMessage(format, args));
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
