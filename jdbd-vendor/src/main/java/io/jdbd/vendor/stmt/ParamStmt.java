package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultStates;

import java.util.List;
import java.util.function.Consumer;

public interface ParamStmt extends Stmt {

    String getSql();

    Consumer<ResultStates> getStatusConsumer();

    /**
     * @return a unmodifiable list
     */
    List<? extends ParamValue> getParamGroup();


    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();



}
