package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.Option;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * <p>
 * This interface is reactive version of {@code java.sql.Statement}.
 * <p>
 * This interface representing the statement couldn't contain any sql parameter placeholder({@code ?}) .
 * </p>
 * </p>
 */
public interface StaticStatement extends Statement, StaticStatementSpec {


    /**
     * {@inheritDoc }
     */
    @Override
    StaticStatement bindStmtVar(String name, DataType dataType, @Nullable Object value) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    StaticStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    StaticStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    StaticStatement setImportPublisher(Function<ChunkOption, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    StaticStatement setExportSubscriber(Function<ChunkOption, Subscriber<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    <T> StaticStatement setOption(Option<T> option, @Nullable T value) throws JdbdException;


}
