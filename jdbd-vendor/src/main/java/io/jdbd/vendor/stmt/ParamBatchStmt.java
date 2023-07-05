package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.function.Function;

/**
 * <p>
 * This interface representing stmt have only one sql that has parameter placeholder,and is batch.
 * </p>
 *
 * @param <T> ParamValue that is extended by developer of driver for adding {@link io.jdbd.meta.SQLType}.
 */
public interface ParamBatchStmt<T extends ParamValue> extends ParamSingleStmt, BatchStmt {


    List<List<T>> getGroupList();


    /**
     * <p>
     * If {@link #getGroupList()} size isn't 1 ,then always return 0 .
     * </p>
     */
    @Override
    int getFetchSize();

    /**
     * <p>
     * If {@link #getGroupList()} size isn't 1 ,then always return null .
     * </p>
     */
    @Override
    Function<Object, Publisher<byte[]>> getImportPublisher();

    /**
     * <p>
     * If {@link #getGroupList()} size isn't 1 ,then always return null .
     * </p>
     */
    @Override
    Function<Object, Subscriber<byte[]>> getExportSubscriber();


}
