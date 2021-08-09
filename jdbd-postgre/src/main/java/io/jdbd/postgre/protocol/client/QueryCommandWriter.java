package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.Encoding;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class QueryCommandWriter {

    static Iterable<ByteBuf> createStaticSingleCommand(List<Stmt> stmtList, TaskAdjutant adjutant)
            throws SQLException {
        return null;
    }

    static Iterable<ByteBuf> createStaticSingleCommand(Stmt stmt, TaskAdjutant adjutant) {
        final byte[] sqlBytes = stmt.getSql().getBytes(Encoding.CLIENT_CHARSET);

        ByteBuf message = adjutant.allocator().buffer(6 + sqlBytes.length);

        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_SIZE);
        message.writeBytes(sqlBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return Collections.singletonList(message);
    }


}
