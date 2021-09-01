package io.jdbd.vendor.session;

import io.jdbd.DatabaseSession;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import reactor.core.publisher.Mono;

public interface ReactorTxSession extends DatabaseSession {

    @Override
    Mono<PreparedStatement> prepare(String sql);


    @Override
    MultiStatement multi();


}
