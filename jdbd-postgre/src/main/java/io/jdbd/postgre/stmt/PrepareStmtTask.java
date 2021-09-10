package io.jdbd.postgre.stmt;

import io.jdbd.postgre.PgType;
import io.jdbd.result.*;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.List;

public interface PrepareStmtTask {

    Mono<ResultStates> executeUpdate(ParamStmt stmt);

    Flux<ResultRow> executeQuery(ParamStmt stmt);

    Flux<ResultStates> executeBatch(ParamBatchStmt<ParamValue> stmt);

    MultiResult executeBatchAsMulti(ParamBatchStmt<ParamValue> stmt);

    OrderedFlux executeBatchAsFlux(ParamBatchStmt<ParamValue> stmt);

    List<PgType> getParamTypeList();

    @Nullable
    ResultRowMeta getRowMeta();

    void closeOnBindError(Throwable error);

    String getSql();

    Mono<Void> abandonBind();

}
