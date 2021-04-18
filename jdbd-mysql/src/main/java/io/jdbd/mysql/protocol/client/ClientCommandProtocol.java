package io.jdbd.mysql.protocol.client;


import io.jdbd.mysql.stmt.BatchBindWrapper;
import io.jdbd.mysql.stmt.BindableWrapper;
import io.jdbd.result.MultiResults;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.stmt.StmtWrapper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Consumer;

public interface ClientCommandProtocol extends ClientProtocol {

    long getId();

    Mono<ResultStates> update(String sql);

    Flux<ResultRow> query(String sql, Consumer<ResultStates> statesConsumer);

    Flux<ResultStates> batchUpdate(List<String> sqlList);

    Mono<ResultStates> bindableUpdate(BindableWrapper wrapper);

    Flux<ResultRow> bindableQuery(BindableWrapper wrapper);

    Flux<ResultStates> bindableBatch(BatchBindWrapper wrapper);

    Mono<PreparedStatement> prepare(StmtWrapper wrapper);

    MultiResults multiStmt(List<String> commandList);

    MultiResults multiBindable(List<BindableWrapper> wrapperList);

    Mono<Void> reset();

}
