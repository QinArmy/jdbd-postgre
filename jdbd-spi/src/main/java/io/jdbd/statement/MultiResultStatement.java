package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.BatchQuery;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultStates;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.Option;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @see BindStatement
 * @see PreparedStatement
 * @see MultiStatement
 */
public interface MultiResultStatement extends Statement {


    Publisher<ResultStates> executeBatchUpdate();

    BatchQuery executeBatchQuery();

    /**
     * @see BindStatement#executeBatchAsMulti()
     * @see PreparedStatement#executeBatchAsMulti()
     * @see MultiStatement#executeBatchAsMulti()
     */
    MultiResult executeBatchAsMulti();

    /**
     * @see BindStatement#executeBatchAsMulti()
     * @see PreparedStatement#executeBatchAsMulti()
     * @see MultiStatement#executeBatchAsMulti()
     */
    OrderedFlux executeBatchAsFlux();

    /**
     * {@inheritDoc }
     */
    @Override
    MultiResultStatement bindStmtVar(String name, DataType dataType, @Nullable Object value) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    MultiResultStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiResultStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiResultStatement setImportPublisher(Function<ChunkOption, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    MultiResultStatement setExportSubscriber(Function<ChunkOption, Subscriber<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    <T> MultiResultStatement setOption(Option<T> option, @Nullable T value) throws JdbdException;


}
