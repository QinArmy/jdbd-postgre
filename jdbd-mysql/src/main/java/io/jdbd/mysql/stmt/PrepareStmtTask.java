package io.jdbd.mysql.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PrepareStmtTask {

    Mono<ResultStates> executeUpdate(ParamStmt stmt);

    Flux<ResultRow> executeQuery(ParamStmt stmt);

    Flux<ResultStates> executeBatch(ParamBatchStmt<? extends ParamValue> stmt);

    ReactorMultiResult executeAsMulti(ParamBatchStmt<? extends ParamValue> stmt);

    Flux<SingleResult> executeAsFlux(ParamBatchStmt<? extends ParamValue> stmt);

    int getWarnings();

}
