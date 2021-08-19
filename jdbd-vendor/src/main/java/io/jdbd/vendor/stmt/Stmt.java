package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultState;

import java.util.function.Consumer;

public interface Stmt extends StmtOptions {

    String getSql();

    Consumer<ResultState> getStatusConsumer();

}
