package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
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
public class ExtendedQueryTaskSqlTypeSuiteTests extends AbstractStmtTaskTests {


    public ExtendedQueryTaskSqlTypeSuiteTests() {
        super(200);
    }


    @Override
    BiFunction<BindStmt, TaskAdjutant, Mono<ResultStates>> updateFunction() {
        return null;
    }

    @Override
    BiFunction<BindStmt, TaskAdjutant, Flux<ResultRow>> queryFunction() {
        return null;
    }
}
