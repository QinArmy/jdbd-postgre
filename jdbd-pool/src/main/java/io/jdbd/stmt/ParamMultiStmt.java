package io.jdbd.stmt;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.function.Function;

/**
 * <p>
 * This interface representing multi {@link ParamStmt}.
 * This used by {@link io.jdbd.statement.MultiStatement} for wrap sql and params.
 * </p>
 */
public interface ParamMultiStmt extends Stmt {

    List<? extends ParamStmt> getStmtList();


    /**
     * @return always 0 .
     */
    @Override
    int getFetchSize();

    /**
     * @return always null
     */
    @Override
    Function<Object, Publisher<byte[]>> getImportPublisher();

    /**
     * @return always null
     */
    @Override
    Function<Object, Subscriber<byte[]>> getExportSubscriber();


}
