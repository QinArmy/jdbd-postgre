package io.jdbd.postgre.stmt;

import io.jdbd.postgre.PgType;
import io.jdbd.result.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.List;

public interface PrepareStmtTask {

    Mono<ResultStates> executeUpdate(BindStmt stmt);

    Flux<ResultRow> executeQuery(BindStmt stmt);

    Flux<ResultStates> executeBatch(BatchBindStmt stmt);

    MultiResult executeBatchAsMulti(BatchBindStmt stmt);

    Flux<Result> executeBatchAsFlux(BatchBindStmt stmt);

    List<PgType> getParamTypeList();

    @Nullable
    ResultRowMeta getRowMeta();

    void closeOnBindError(Throwable error);

    String getSql();

}
