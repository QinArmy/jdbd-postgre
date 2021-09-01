package io.jdbd.postgre.stmt;

import io.jdbd.result.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.List;

public interface PrepareStmtTask {

    Mono<ResultStates> executeUpdate(BindStmt stmt);

    Flux<ResultRow> executeQuery(BindStmt stmt);

    Flux<ResultStates> executeBatch(BatchBindStmt stmt);

    MultiResult executeAsMulti(BatchBindStmt stmt);

    Flux<Result> executeAsFlux(BatchBindStmt stmt);

    List<Integer> getParamTypeOidList();

    @Nullable
    ResultRowMeta getRowMeta();

    void closeOnBindError();

    String getSql();

}
