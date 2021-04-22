package io.jdbd.mysql.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.result.SingleResult;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.BatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PrepareStmtTask {

    Mono<ResultStatus> executeUpdate(ParamStmt stmt);

    Flux<ResultRow> executeQuery(ParamStmt stmt);

    Flux<ResultStatus> executeBatch(BatchStmt<? extends ParamValue> stmt);

    ReactorMultiResult executeAsMulti(BatchStmt<? extends ParamValue> stmt);

    Flux<SingleResult> executeAsFlux(BatchStmt<? extends ParamValue> stmt);

    int getWarnings();

}
