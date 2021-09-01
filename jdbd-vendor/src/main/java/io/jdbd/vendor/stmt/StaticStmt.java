package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultStates;

import java.util.function.Consumer;

public interface StaticStmt extends Stmt {

    String getSql();

    Consumer<ResultStates> getStatusConsumer();


    int getFetchSize();


}
