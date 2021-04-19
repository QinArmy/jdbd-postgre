package io.jdbd.mysql.stmt;

import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.stmt.BatchWrapper;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.ParamWrapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PrepareStmtTask {

    Mono<ResultStates> executeUpdate(ParamWrapper wrapper);

    Flux<ResultRow> executeQuery(ParamWrapper wrapper);

    Flux<ResultStates> executeBatch(BatchWrapper<? extends ParamValue> wrapper);

    ReactorMultiResult executeMulti(ParamWrapper wrapper);

    ReactorMultiResult executeBatchMulti(BatchWrapper<? extends ParamValue> wrapper);

    int getWarnings();

}
