package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

public interface OneStepPrepareStatement extends ServerPrepareStatement {

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bindStmtVar(String name, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement bindStmtVar(String name, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement addBatch() throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    OneStepPrepareStatement setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException;


}
