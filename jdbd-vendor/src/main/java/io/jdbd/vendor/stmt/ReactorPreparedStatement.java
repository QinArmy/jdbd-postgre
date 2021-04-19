package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;


/**
 * <p>
 * This interface override return type below methods to avoid type error:
 *     <ul>
 *         <li>{@link #executeBatch()}</li>
 *         <li>{@link #executeUpdate()}</li>
 *         <li>{@link #executeQuery()}</li>
 *         <li>{@link #executeQuery(Consumer)}</li>
 *         <li>{@link #executeMulti()}</li>
 *         <li>{@link #executeBatchMulti()}</li>
 *     </ul>
 * </p>
 */
public interface ReactorPreparedStatement extends PreparedStatement {

    @Override
    Flux<ResultStates> executeBatch();

    @Override
    Mono<ResultStates> executeUpdate();

    @Override
    Flux<ResultRow> executeQuery();

    @Override
    Flux<ResultRow> executeQuery(Consumer<ResultStates> statesConsumer);

    @Override
    ReactorMultiResult executeMulti();

    @Override
    ReactorMultiResult executeBatchMulti();


}
