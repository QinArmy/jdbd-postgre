package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.client.ClientCommandProtocol;
import io.jdbd.mysql.stmt.Stmts;
import io.jdbd.stmt.BindStatement;
import io.jdbd.stmt.MultiStatement;
import io.jdbd.stmt.PreparedStatement;
import io.jdbd.stmt.StaticStatement;
import io.jdbd.vendor.session.ReactorDatabaseSession;
import reactor.core.publisher.Mono;

public abstract class MySQLDatabaseSession implements ReactorDatabaseSession {

    final ClientCommandProtocol protocol;


    MySQLDatabaseSession(ClientCommandProtocol protocol) {
        this.protocol = protocol;

    }


    @Override
    public final StaticStatement statement() {
        return MySQLStaticStatement.create(this);
    }

    @Override
    public final Mono<PreparedStatement> prepare(String sql) {
        return this.protocol.prepare(this, Stmts.stmt(sql));
    }


    @Override
    public final BindStatement bindable(String sql) {
        return null;
    }

    @Override
    public final MultiStatement multi() {
        return null;
    }


}
