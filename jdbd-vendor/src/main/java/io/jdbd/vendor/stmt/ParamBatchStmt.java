package io.jdbd.vendor.stmt;

import java.util.List;

/**
 * <p>
 * This interface representing stmt have only one sql that has parameter placeholder,and is batch.
 * </p>
 *
 * @param <T> ParamValue that is extended by developer of driver for adding {@link io.jdbd.meta.SQLType}.
 */
public interface ParamBatchStmt<T extends ParamValue> extends ParamSingleStmt {


    List<List<T>> getGroupList();


}
