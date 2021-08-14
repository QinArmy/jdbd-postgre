package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.List;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class QueryCommandWriter {

    static Publisher<ByteBuf> createStaticBatchCommand(GroupStmt stmt, TaskAdjutant adjutant)
            throws SQLException, JdbdException {
        final List<String> sqlGroup = stmt.getSqlGroup();
        final ByteBuf message = adjutant.allocator().buffer(sqlGroup.size() * 50, Integer.MAX_VALUE);
        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_SIZE); // placeholder
        try {
            final Charset charset = adjutant.clientCharset();
            final byte[] semicolonBytes = ";".getBytes(charset);
            int count = 0;
            for (String sql : sqlGroup) {
                if (!adjutant.isSingleStmt(sql)) {
                    throw PgExceptions.createMultiStatementError();
                }
                if (count > 0) {
                    message.writeBytes(semicolonBytes);
                }
                message.writeBytes(sql.getBytes(charset));
                count++;
            }
            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return Mono.just(message);
        } catch (Throwable e) {
            message.release();

            if (e instanceof SQLException) {
                throw e;
            } else if (e instanceof IndexOutOfBoundsException) {
                throw PgExceptions.createObjectTooLargeError();
            } else {
                throw PgExceptions.wrap(e);
            }
        }
    }

    static Publisher<ByteBuf> createStaticSingleCommand(Stmt stmt, TaskAdjutant adjutant) throws SQLException {
        final byte[] sqlBytes = stmt.getSql().getBytes(adjutant.clientCharset());
        final int capacity = sqlBytes.length + 6;
        if (capacity < 0) {
            throw PgExceptions.createObjectTooLargeError();
        }
        final ByteBuf message = adjutant.allocator().buffer(capacity);

        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_SIZE); // placeholder
        message.writeBytes(sqlBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return Mono.just(message);
    }


}
