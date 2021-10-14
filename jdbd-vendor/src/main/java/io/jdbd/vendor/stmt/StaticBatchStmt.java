package io.jdbd.vendor.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.function.Function;

/**
 * <p>
 * This interface representing stmt have only multi sql,and no parameter placeholder.
 * This implementation of this interface is used by the implementation of below methods:
 * <u>
 * <li>{@link io.jdbd.stmt.StaticStatement#executeUpdate(String)}</li>
 * <li>{@link io.jdbd.stmt.StaticStatement#executeQuery(String)}</li>
 * <li>{@link io.jdbd.stmt.StaticStatement#executeQuery(String, java.util.function.Consumer)}</li>
 * </u>
 * </p>
 */
public interface StaticBatchStmt extends Stmt {

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
    @Override
    Function<Object, Publisher<byte[]>> getImportPublisher();

    /**
     * <p>
     * If {@link #getSqlGroup()} size isn't 1 ,then always return null .
     * </p>
     */
    @Override
    Function<Object, Subscriber<byte[]>> getExportSubscriber();

}
