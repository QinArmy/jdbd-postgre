package io.jdbd.mysql.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultState;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.BatchParamStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PrepareStmtTask {

    Mono<ResultState> executeUpdate(ParamStmt stmt);

    Flux<ResultRow> executeQuery(ParamStmt stmt);

    Flux<ResultState> executeBatch(BatchParamStmt<? extends ParamValue> stmt);

    ReactorMultiResult executeAsMulti(BatchParamStmt<? extends ParamValue> stmt);

    Flux<SingleResult> executeAsFlux(BatchParamStmt<? extends ParamValue> stmt);

    int getWarnings();

}
