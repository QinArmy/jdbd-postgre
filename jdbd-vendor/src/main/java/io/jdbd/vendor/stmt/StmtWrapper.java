package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultStatus;

import java.util.function.Consumer;

public interface StmtWrapper {

    String getSql();

    int getTimeout();

    Consumer<ResultStatus> getStatusConsumer();

}
