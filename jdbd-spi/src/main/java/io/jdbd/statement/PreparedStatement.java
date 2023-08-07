package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.RefCursor;
import io.jdbd.result.ResultRowMeta;
import io.jdbd.result.Warning;
import io.jdbd.session.ChunkOption;
import io.jdbd.session.DatabaseSession;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * <p>
 * This interface is reactive version of {@code java.sql.PreparedStatement}
 * </p>
 * <p>
 * You must invoke one of following :
 * <ul>
 *     <li>{@link #executeUpdate()}</li>
 *     <li>{@link #executeQuery()}</li>
 *     <li>{@link #executeQuery(Function)}</li>
 *     <li>{@link #executeQuery(Function, Consumer)}</li>
 *     <li>{@link #executeBatchUpdate()}</li>
 *     <li>{@link #executeBatchQuery()}</li>
 *     <li>{@link #executeBatchAsMulti()}</li>
 *     <li>{@link #executeBatchAsFlux()}</li>
 *     <li>{@link #executeCursor()}, you must invoke {@link RefCursor#close()} after {@link #executeCursor()} success</li>
 *     <li>{@link #abandonBind()}</li>
 * </ul>
 * or {@link DatabaseSession} of this {@link PreparedStatement}
 * can't execute any new {@link Statement},because this session will wait(maybe in task queue)
 * for you invoke one of above.
 * </p>
 * <p>
 *     <strong>NOTE</strong>: {@link PreparedStatement} is auto close after you invoke executeXxx() method,or binding occur error,so
 *     {@link PreparedStatement} have no close() method.
 * </p>
 *
 * @see BindStatement
 */
public interface PreparedStatement extends BindSingleStatement {

    @Nullable
    ResultRowMeta resultRowMeta();

    List<? extends DataType> paramTypeList();


    @Nullable
    Warning waring();

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement bind(int indexBasedZero, DataType dataType, @Nullable Object value) throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement bindStmtVar(String name, DataType dataType, @Nullable Object value) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement addBatch() throws JdbdException;


    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement setTimeout(int seconds) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement setFetchSize(int fetchSize) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement setImportPublisher(Function<ChunkOption, Publisher<byte[]>> function) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    PreparedStatement setExportSubscriber(Function<ChunkOption, Subscriber<byte[]>> function) throws JdbdException;


    /**
     * <p>
     * This method close this  {@link PreparedStatement} if you don't invoke any executeXxx() method.
     * </p>
     * <p>
     * Abandon binding before invoke executeXxx() method.
     * </p>
     *
     * @return Publisher like {@code reactor.core.publisher.Mono} ,
     * if success emit {@link DatabaseSession} that create this {@link PreparedStatement}.
     * @throws JdbdException emit(not throw), when after invoking executeXxx().
     */
    Publisher<DatabaseSession> abandonBind();


}
