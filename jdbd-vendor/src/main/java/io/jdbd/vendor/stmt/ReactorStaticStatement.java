package io.jdbd.vendor.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
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
 *         <li>{@link #executeMulti(String)}</li>
 *     </ul>
 * </p>
 */
public interface ReactorStaticStatement extends StaticStatement {

    @Override
    Flux<ResultStates> executeBatch(List<String> sqlList);

    @Override
    Mono<ResultStates> executeUpdate(String sql);

    @Override
    Flux<ResultRow> executeQuery(String sql);

    @Override
    Flux<ResultRow> executeQuery(String sql, Consumer<ResultStates> statesConsumer);

    @Override
    ReactorMultiResult executeMulti(String sql);


    @Override
    ReactorMultiResult executeBatchMulti(String sql);
}
