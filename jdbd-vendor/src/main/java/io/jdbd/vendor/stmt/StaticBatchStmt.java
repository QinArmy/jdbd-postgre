package io.jdbd.vendor.stmt;

import io.jdbd.lang.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface representing stmt have only multi sql,and no parameter placeholder.
 * This implementation of this interface is used by the implementation of below methods:
 * <u>
 * <li>{@link io.jdbd.statement.StaticStatement#executeUpdate(String)}</li>
 * <li>{@link io.jdbd.statement.StaticStatement#executeQuery(String, Function)}</li>
 * <li>{@link io.jdbd.statement.StaticStatement#executeQuery(String, Function, Consumer)}</li>
 * </u>
 * </p>
 */
public interface StaticBatchStmt extends BatchStmt {

    /**
     * @return a unmodified list
     */
    List<String> getSqlGroup();


    @Override
    int getTimeout();

    /**
     * <p>
     * If {@link #getSqlGroup()} size isn't 1 ,then always return 0 .
     * </p>
     */
    @Override
    int getFetchSize();

    /**
     * <p>
     * If {@link #getSqlGroup()} size isn't 1 ,then always return null .
     * </p>
     */
    @Nullable
    @Override
    Function<Object, Publisher<byte[]>> getImportPublisher();

    /**
     * <p>
     * If {@link #getSqlGroup()} size isn't 1 ,then always return null .
     * </p>
     */
    @Nullable
    @Override
    Function<Object, Subscriber<byte[]>> getExportSubscriber();

}
