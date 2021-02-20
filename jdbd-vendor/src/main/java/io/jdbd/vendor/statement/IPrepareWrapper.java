package io.jdbd.vendor.statement;

import io.jdbd.vendor.IBindValue;

import java.util.List;

public interface IPrepareWrapper<T extends IBindValue> extends StatementWrapper {


    List<List<T>> getParameterGroupList();

    List<T> getParameterGroup();


    /**
     * @return negative or fetch size, if zero ignore.
     */
    int getFetchSize();
}
