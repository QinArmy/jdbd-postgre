package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.function.Function;

/**
 * <p>
 * This interface representing stmt have only multi sql,and no parameter placeholder.
 * This implementation of this interface is used by the implementation of below methods:
 * <u>
 * <li>{@link io.jdbd.statement.StaticStatement#executeUpdate(String)}</li>
 * <li>{@link io.jdbd.statement.StaticStatement#executeQuery(String)}</li>
 * <li>{@link io.jdbd.statement.StaticStatement#executeQuery(String, java.util.function.Consumer)}</li>
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
