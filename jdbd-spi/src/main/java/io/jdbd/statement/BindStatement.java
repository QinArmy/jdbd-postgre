package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * <p>
 * This interface representing the adaptor of client-prepared statement and server-prepared statement.
 * </p>
 *
 * @see io.jdbd.session.DatabaseSession#bindStatement(String, boolean)
 * @since 1.0
 */
public interface BindStatement extends BindSingleStatement {


    /**
     * @return true : must use server prepare statement.
     * @see io.jdbd.session.DatabaseSession#bindStatement(String, boolean)
     */
    boolean isForcePrepare();


    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bind(int indexBasedZero, DataType dataType, @Nullable Object value) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement bindStmtVar(String name, DataType dataType, @Nullable Object value) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement addBatch() throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    BindStatement setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException;


}
