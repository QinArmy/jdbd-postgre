package io.jdbd.stmt;

import io.jdbd.JdbdNonSQLException;

public final class TooManyResultException extends JdbdNonSQLException {

    private final int expectResultCount;

    private final int receiveResultCount;

    public TooManyResultException(int expectResultCount, int receiveResultCount) {
        super(String.format("expect %s result(s),but receive %s results.", expectResultCount, receiveResultCount));
        this.expectResultCount = expectResultCount;
        this.receiveResultCount = receiveResultCount;
    }

    public int getExpectResultCount() {
        return this.expectResultCount;
    }

    public int getReceiveResultCount() {
        return this.receiveResultCount;
    }
}
