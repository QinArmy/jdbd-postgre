package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.vendor.result.ReactorMultiResult;
import io.jdbd.vendor.session.ReactorDatabaseSession;
import io.jdbd.vendor.stmt.ReactorBindableStatement;
import io.jdbd.vendor.stmt.ReactorMultiStatement;
import io.jdbd.vendor.stmt.ReactorStaticStatement;
import reactor.core.publisher.Mono;

import java.util.List;

public abstract class MySQLDatabaseSession implements ReactorDatabaseSession {

    final ClientCommandProtocol protocol;


    MySQLDatabaseSession(ClientCommandProtocol protocol) {
        this.protocol = protocol;

    }


    @Override
    public final ReactorStaticStatement statement() {
        return MySQLStaticStatement.create(this);
    }

    @Override
    public final Mono<PreparedStatement> prepare(String sql) {
        return this.protocol.prepare(this, Stmts.stmt(sql));
    }

    @Override
    public final Mono<PreparedStatement> prepare(String sql, int executeTimeout) {
        return this.protocol.prepare(this, Stmts.stmt(sql, executeTimeout));
    }

    @Override
    public final ReactorBindableStatement bindable(String sql) {
        return null;
    }

    @Override
    public final ReactorMultiStatement multi() {
        return null;
    }

    @Override
    public final ReactorMultiResult multi(List<String> sqlList) {
        return null;
    }


}
