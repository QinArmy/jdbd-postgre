package io.jdbd.vendor.statement;

import io.jdbd.ResultStates;
import io.jdbd.vendor.IBindValue;

import java.util.List;
import java.util.function.Consumer;

public interface IStmtWrapper<T extends IBindValue> {

    String getSql();

    Consumer<ResultStates> getStatesConsumer();

    List<T> getParameterGroup();


    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();
}
