package io.jdbd.vendor;

import io.jdbd.ResultStates;

import java.util.List;
import java.util.function.Consumer;

public interface IStatementWrapper<T extends IBindValue> {

    String getSql();

    List<List<T>> getParameterGroupList();

    List<T> getParameterGroup();

    Consumer<ResultStates> getStatesConsumer();

    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();
}
