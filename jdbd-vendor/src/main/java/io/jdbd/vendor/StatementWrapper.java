package io.jdbd.vendor;

import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.lang.Nullable;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface StatementWrapper<T extends BindValue> {

    String getSql();

    List<List<T>> getParameterGroupList();

    @Nullable
    BiFunction<ResultRow, ResultRowMeta, Object> getFunction();

    @Nullable
    Consumer<ResultStates> getStatesConsumer();
}
