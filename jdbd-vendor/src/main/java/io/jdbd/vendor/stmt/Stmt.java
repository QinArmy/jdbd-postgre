package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultState;

import java.util.function.Consumer;

public interface Stmt {

    String getSql();

    int getTimeout();

    Consumer<ResultState> getStatusConsumer();

}
