package io.jdbd.vendor.statement;

import io.jdbd.ResultStates;

import java.util.function.Consumer;

public interface StatementWrapper {

    String getSql();

    Consumer<ResultStates> getStatesConsumer();
}
