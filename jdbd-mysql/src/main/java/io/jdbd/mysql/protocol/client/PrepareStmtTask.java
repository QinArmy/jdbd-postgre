package io.jdbd.mysql.protocol.client;

import io.jdbd.ResultRow;
import io.jdbd.ResultStates;
import io.jdbd.vendor.stmt.BatchWrapper;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.ParamWrapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface PrepareStmtTask {

    Mono<ResultStates> executeUpdate(ParamWrapper wrapper);

    Flux<ResultRow> executeQuery(ParamWrapper wrapper);

    Flux<ResultStates> executeBatch(BatchWrapper<? extends ParamValue> wrapper);

}
