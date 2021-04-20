package io.jdbd.mysql.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStatus;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.BatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PrepareStmtTask {

    Mono<ResultStatus> executeUpdate(ParamStmt wrapper);

    Flux<ResultRow> executeQuery(ParamStmt wrapper);

    Flux<ResultStatus> executeBatch(BatchStmt<? extends ParamValue> wrapper);

    ReactorMultiResult executeMulti(ParamStmt wrapper);

    ReactorMultiResult executeBatchMulti(BatchStmt<? extends ParamValue> wrapper);

    int getWarnings();

}
