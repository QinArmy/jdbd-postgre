package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgJdbdException;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.jdbd.vendor.syntax.SQLParser;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class QueryCommandWriter {


    static Publisher<ByteBuf> createStaticBatchCommand(GroupStmt stmt, TaskAdjutant adjutant)
            throws Throwable {
        final List<String> sqlGroup = stmt.getSqlGroup();
        final ByteBuf message = adjutant.allocator().buffer(sqlGroup.size() * 50, Integer.MAX_VALUE);
        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_SIZE); // placeholder of length
        try {
            final Charset charset = adjutant.clientCharset();
            final byte[] semicolonBytes = SEMICOLON.getBytes(charset);
            final SQLParser sqlParser = adjutant.sqlParser();
            int count = 0;
            for (String sql : sqlGroup) {
                if (!sqlParser.isSingleStmt(sql)) {
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
            throw convertError(e);
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


    static Publisher<ByteBuf> createBindableCommand(BindableStmt stmt, TaskAdjutant adjutant)
            throws Throwable {
        return new QueryCommandWriter(adjutant)
                .writeCommand(Collections.singletonList(stmt));

    }

    static Publisher<ByteBuf> createBindableBatchCommand(BatchBindStmt stmt, TaskAdjutant adjutant) {
        return Mono.empty();
    }

    static Publisher<ByteBuf> createMultiStmtCommand(MultiBindStmt stmt, TaskAdjutant adjutant) {
        return Mono.empty();
    }

    private static Throwable convertError(Throwable e) {
        final Throwable t;
        if (e instanceof SQLException || JdbdExceptions.isJvmFatal(e)) {
            t = e;
        } else if (e instanceof IndexOutOfBoundsException) {
            t = PgExceptions.createObjectTooLargeError();
        } else {
            t = PgExceptions.wrap(e);
        }
        return t;
    }

    private static final String SEMICOLON = ";";

    private static final String NULL = "NULL";


    private final TaskAdjutant adjutant;

    private final Charset clientCharset;

    private QueryCommandWriter(TaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.clientCharset = adjutant.clientCharset();
    }


    private Publisher<ByteBuf> writeCommand(final List<BindableStmt> stmtList) throws Throwable {
        final ByteBuf message = this.adjutant.allocator().buffer(stmtList.size() << 7, Integer.MAX_VALUE);

        try {
            message.writeByte(Messages.Q);
            message.writeZero(Messages.LENGTH_SIZE); // placeholder

            final Charset charset = this.adjutant.clientCharset();
            final byte[] semicolonBytes = SEMICOLON.getBytes(charset);
            final PgParser sqlParser = this.adjutant.sqlParser();
            int count = 0;
            PgStatement statement;
            for (BindableStmt stmt : stmtList) {
                statement = sqlParser.parse(stmt.getSql());
                if (count > 0) {
                    message.writeBytes(semicolonBytes);
                }
                writeStatement(statement, stmt.getParamGroup(), message);
                count++;
            }
            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return Mono.just(message);
        } catch (Throwable e) {
            message.release();
            throw convertError(e);
        }
    }

    private void writeStatement(PgStatement statement, final List<BindValue> valueList, ByteBuf message) {
        final List<String> staticSqlList = statement.getStaticSql();
        final int paramCount = staticSqlList.size() - 1;
        if (valueList.size() != paramCount) {
            throw new PgJdbdException(String.format(""));
        }
        final Charset clientCharset = this.clientCharset;
        BindValue bindValue;
        for (int i = 0; i < paramCount; i++) {
            bindValue = valueList.get(i);
            if (bindValue.getParamIndex() != i) {
                throw new PgJdbdException("");
            }
            message.writeBytes(staticSqlList.get(i).getBytes(clientCharset));

        }
        message.writeBytes(staticSqlList.get(paramCount).getBytes(clientCharset));

    }


}
