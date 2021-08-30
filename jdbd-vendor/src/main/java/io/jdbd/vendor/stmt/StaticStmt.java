package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultState;

import java.util.function.Consumer;

public interface StaticStmt extends Stmt {

    String getSql();

    Consumer<ResultState> getStatusConsumer();




}
