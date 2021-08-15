package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.BindableStatement;
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
 *         <li>{@link #executeAsMulti()}</li>
 *         <li>{@link #executeAsFlux()}</li>
 *     </ul>
 * </p>
 */
@Deprecated
public interface ReactorBindableStatement extends BindableStatement {

    @Override
    Flux<ResultState> executeBatch();

    @Override
    Mono<ResultState> executeUpdate();

    @Override
    Flux<ResultRow> executeQuery();

    @Override
    Flux<ResultRow> executeQuery(Consumer<ResultState> statesConsumer);

    @Override
    ReactorMultiResult executeAsMulti();

    @Override
    Flux<SingleResult> executeAsFlux();

}
