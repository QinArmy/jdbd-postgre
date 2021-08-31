package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;

/**
 * <p>
 * This class is test class of {@link ExtendedQueryTask}.
 * </p>
 *
 * @see ExtendedQueryTask
 */
public class ExtendedQueryTaskSuiteTests extends AbstractStmtTaskTests {


    public ExtendedQueryTaskSuiteTests() {
        super(200);
    }


    @Override
    BiFunction<BindStmt, TaskAdjutant, Mono<ResultState>> updateFunction() {
        return null;
    }

    @Override
    BiFunction<BindStmt, TaskAdjutant, Flux<ResultRow>> queryFunction() {
        return null;
    }
}
