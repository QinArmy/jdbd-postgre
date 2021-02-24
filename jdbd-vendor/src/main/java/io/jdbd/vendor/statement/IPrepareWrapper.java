package io.jdbd.vendor.statement;

import io.jdbd.vendor.IBindValue;

import java.util.List;

public interface IPrepareWrapper<T extends IBindValue> extends IStmtWrapper<T> {


    List<List<T>> getParameterGroupList();

}
