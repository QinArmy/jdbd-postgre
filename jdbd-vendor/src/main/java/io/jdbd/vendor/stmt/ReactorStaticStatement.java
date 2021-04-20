package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;


/**
 * <p>
 * This interface override return type below methods to avoid type error:
 *     <ul>
 *         <li>{@link #executeBatch(List)}</li>
 *         <li>{@link #executeUpdate(String)}</li>
 *         <li>{@link #executeQuery(String)}</li>
 *         <li>{@link #executeQuery(String, Consumer)}</li>
 *         <li>{@link #executeAsMulti(List)}</li>
 *         <li>{@link #executeAsFlux(List)}</li>
 *     </ul>
 * </p>
 */
public interface ReactorStaticStatement extends StaticStatement {

    @Override
    Flux<ResultStatus> executeBatch(List<String> sqlList);

    @Override
    Mono<ResultStatus> executeUpdate(String sql);

    @Override
    Flux<ResultRow> executeQuery(String sql);

    @Override
    Flux<ResultRow> executeQuery(String sql, Consumer<ResultStatus> statesConsumer);

    @Override
    ReactorMultiResult executeAsMulti(List<String> sqlList);

    @Override
    Flux<SingleResult> executeAsFlux(List<String> sqlList);
}
