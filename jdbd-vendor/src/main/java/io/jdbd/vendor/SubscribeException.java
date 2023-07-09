package io.jdbd.vendor;

import io.jdbd.JdbdException;

public final class SubscribeException extends JdbdException {


    private final ResultType subscribeType;

    private final ResultType actualType;

    public SubscribeException(ResultType subscribeType, ResultType actualType) {
        super(String.format("Subscribe ResultType[%s] but actual ResultType[%s]", subscribeType, actualType));
        this.subscribeType = subscribeType;
        this.actualType = actualType;
    }

    public SubscribeException(ResultType subscribeType, ResultType actualType, String message) {
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
