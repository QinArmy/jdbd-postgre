package io.jdbd.vendor.statement;

import io.jdbd.ResultStates;

import java.util.List;
import java.util.function.Consumer;

public interface IStmtWrapper<T extends BindValue> {

    String getSql();

    Consumer<ResultStates> getStatesConsumer();

    List<T> getParameterGroup();


    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();
}
