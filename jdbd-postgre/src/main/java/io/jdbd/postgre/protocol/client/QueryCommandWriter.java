package io.jdbd.postgre.protocol.client;

import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;

import java.sql.SQLException;
import java.util.List;

final class QueryCommandWriter {

    static Iterable<ByteBuf> createStaticSingleCommand(List<Stmt> stmtList, TaskAdjutant adjutant)
            throws SQLException {
        return null;
    }
}
