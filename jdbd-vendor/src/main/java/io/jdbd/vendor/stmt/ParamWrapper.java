package io.jdbd.vendor.stmt;

import io.jdbd.ResultStates;

import java.util.List;
import java.util.function.Consumer;

public interface ParamWrapper extends StmtWrapper {

    /**
     * @return a unmodifiable list
     */
    List<? extends ParamValue> getParamGroup();

    Consumer<ResultStates> getStatesConsumer();

    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();



}
