package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    final Mono<ResultStates> executeUpdate(BindStmt stmt, TaskAdjutant adjutant) {
        return ExtendedQueryTask.update(stmt, adjutant);
    }

    @Override
    final Flux<ResultRow> executeQuery(BindStmt stmt, TaskAdjutant adjutant) {
        return ExtendedQueryTask.query(stmt, adjutant);
    }


}
