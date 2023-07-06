package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * <p>
 * This interface is base interface of following:
 *     <ul>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link OneStepPrepareStatement}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface ServerPrepareStatement extends BindSingleStatement {


    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement addBatch() throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException;


}
