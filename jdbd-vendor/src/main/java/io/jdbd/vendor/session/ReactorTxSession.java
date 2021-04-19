package io.jdbd.vendor.session;

import io.jdbd.DatabaseSession;
import io.jdbd.result.MultiResult;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import reactor.core.publisher.Mono;

import java.util.List;

public interface ReactorTxSession extends DatabaseSession {

    @Override
    Mono<PreparedStatement> prepare(String sql);

    @Override
    Mono<PreparedStatement> prepare(String sql, int executeTimeout);

    @Override
    MultiStatement multi();

    @Override
    MultiResult multi(List<String> sqlList);


}
