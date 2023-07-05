package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.MultiResult;
import io.jdbd.result.OrderedFlux;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.DatabaseSession;
import org.reactivestreams.Publisher;

import java.sql.JDBCType;
import java.util.function.Consumer;

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

    @Override
    boolean supportPublisher();

    @Override
    boolean supportOutParameter();


    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    @Override
    ServerPrepareStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    @Override
    ServerPrepareStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;

    @Override
    ServerPrepareStatement bind(int indexBasedZero, String dataTypeName, @Nullable Object nullable) throws JdbdException;

    /**
     * {@inheritDoc }
     */
    @Override
    ServerPrepareStatement addBatch() throws JdbdException;

    @Override
    Publisher<ResultStates> executeUpdate();

    /**
     * @see #executeQuery(Consumer)
     */
    @Override
    Publisher<ResultRow> executeQuery();

    @Override
    Publisher<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer);

    @Override
    Publisher<ResultStates> executeBatchUpdate();

    @Override
    MultiResult executeBatchAsMulti();

    OrderedFlux executeBatchAsFlux();

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
