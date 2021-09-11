package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.stmt.BindBatchStmt;
import io.jdbd.postgre.stmt.BindMultiStmt;
import io.jdbd.postgre.stmt.BindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgBinds;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.syntax.SQLParser;
import io.jdbd.vendor.util.JdbdBuffers;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.*;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class QueryCommandWriter {


    static Publisher<ByteBuf> createStaticBatchCommand(StaticBatchStmt stmt, TaskAdjutant adjutant)
            throws Throwable {
        final List<String> sqlGroup = stmt.getSqlGroup();
        final ByteBuf message = adjutant.allocator().buffer(sqlGroup.size() * 50, Integer.MAX_VALUE);
        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_BYTES); // placeholder of length
        try {
            final Charset charset = adjutant.clientCharset();
            final SQLParser sqlParser = adjutant.sqlParser();
            int count = 0;
            for (String sql : sqlGroup) {
                if (!sqlParser.isSingleStmt(sql)) {
                    throw PgExceptions.createMultiStatementError();
                }
                if (count > 0) {
                    message.writeByte(SEMICOLON_BYTE);
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


    static Publisher<ByteBuf> createStaticCommand(String sql, TaskAdjutant adjutant) throws SQLException {
        final byte[] sqlBytes = sql.getBytes(adjutant.clientCharset());
        final int capacity = sqlBytes.length + 6;
        if (capacity < 0) {
            throw PgExceptions.createObjectTooLargeError();
        }
        final ByteBuf message = adjutant.allocator().buffer(capacity);

        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_BYTES); // placeholder
        message.writeBytes(sqlBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return Mono.just(message);
    }


    static Publisher<ByteBuf> createBindableCommand(BindStmt stmt, TaskAdjutant adjutant)
            throws Throwable {
        ByteBuf message;
        message = new QueryCommandWriter(adjutant)
                .writeMultiBindCommand(Collections.singletonList(stmt));
        return Mono.just(message);
    }

    static Publisher<ByteBuf> createBindableBatchCommand(BindBatchStmt stmt, TaskAdjutant adjutant)
            throws Throwable {
        ByteBuf message;
        message = new QueryCommandWriter(adjutant)
                .writeBatchBindCommand(stmt);
        return Mono.just(message);
    }

    static Publisher<ByteBuf> createMultiStmtCommand(BindMultiStmt stmt, TaskAdjutant adjutant) throws Throwable {
        ByteBuf message;
        message = new QueryCommandWriter(adjutant)
                .writeMultiBindCommand(stmt.getStmtGroup());
        return Mono.just(message);
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

    private static final Logger LOG = LoggerFactory.getLogger(QueryCommandWriter.class);

    private static final String NULL = "NULL", TRUE = "TRUE", FALSE = "FALSE";

    private static final byte BACK_SLASH_BYTE = '\\';

    private static final byte QUOTE_BYTE = '\'';

    private static final byte SEMICOLON_BYTE = ';';

    private final TaskAdjutant adjutant;

    private final Charset clientCharset;

    private QueryCommandWriter(TaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.clientCharset = adjutant.clientCharset();
    }

    /**
     * @see #createMultiStmtCommand(BindMultiStmt, TaskAdjutant)
     * @see #createBindableCommand(BindStmt, TaskAdjutant)
     */
    private ByteBuf writeMultiBindCommand(final List<BindStmt> stmtList) throws Throwable {
        final TaskAdjutant adjutant = this.adjutant;
        int capacity = stmtList.size() << 7;
        if (capacity < 0) {
            capacity = Integer.MAX_VALUE;
        }
        final ByteBuf message = adjutant.allocator().buffer(capacity, Integer.MAX_VALUE);

        try {
            message.writeByte(Messages.Q);
            message.writeZero(Messages.LENGTH_BYTES); // placeholder

            final PgParser sqlParser = adjutant.sqlParser();
            PgStatement statement;
            BindStmt stmt;
            final int stmtCount = stmtList.size();
            for (int i = 0; i < stmtCount; i++) {
                stmt = stmtList.get(i);
                statement = sqlParser.parse(stmt.getSql());
                if (i > 0) {
                    message.writeByte(SEMICOLON_BYTE);
                }
                writeStatement(i, statement, stmt.getBindGroup(), message);
            }

            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return message;
        } catch (Throwable e) {
            message.release();
            throw convertError(e);
        }
    }

    /**
     * @see #createBindableBatchCommand(BindBatchStmt, TaskAdjutant)
     */
    private ByteBuf writeBatchBindCommand(BindBatchStmt stmt) throws Throwable {
        final TaskAdjutant adjutant = this.adjutant;
        final String sql = stmt.getSql();
        final List<List<BindValue>> groupList = stmt.getGroupList();
        final int stmtCount = groupList.size();

        int capacity = (sql.length() + 40) * stmtCount;
        if (capacity < 0) {
            capacity = Integer.MAX_VALUE;
        }
        final ByteBuf message = adjutant.allocator().buffer(capacity, Integer.MAX_VALUE);
        try {
            message.writeByte(Messages.Q);
            message.writeZero(Messages.LENGTH_BYTES); // placeholder

            final PgStatement statement;
            statement = adjutant.sqlParser().parse(sql);
            for (int i = 0; i < stmtCount; i++) {
                if (i > 0) {
                    message.writeByte(SEMICOLON_BYTE);
                }
                writeStatement(i, statement, groupList.get(i), message);
            }

            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return message;
        } catch (Throwable e) {
            message.release();
            throw convertError(e);
        }
    }


    /**
     * @see #writeMultiBindCommand(List)
     * @see #writeBatchBindCommand(BindBatchStmt)
     */
    private void writeStatement(final int stmtIndex, PgStatement statement, List<BindValue> valueList, ByteBuf message)
            throws SQLException, IOException {

        final List<String> staticSqlList = statement.getStaticSql();
        final int paramCount = staticSqlList.size() - 1;
        if (valueList.size() != paramCount) {
            throw PgExceptions.createBindCountNotMatchError(stmtIndex, paramCount, valueList.size());
        }
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = NULL.getBytes(clientCharset);
        BindValue bindValue;
        for (int i = 0; i < paramCount; i++) {
            bindValue = valueList.get(i);
            if (bindValue.getIndex() != i) {
                throw PgExceptions.createBindIndexNotMatchError(stmtIndex, i, bindValue);
            }
            message.writeBytes(staticSqlList.get(i).getBytes(clientCharset));
            if (bindValue.get() == null) {
                message.writeBytes(nullBytes);
            } else {
                bindNonNullParameter(stmtIndex, bindValue, message);
            }

        }
        message.writeBytes(staticSqlList.get(paramCount).getBytes(clientCharset));

    }


    /**
     * @see #writeStatement(int, PgStatement, List, ByteBuf)
     */
    private void bindNonNullParameter(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException, IOException {

        switch (bindValue.getType()) {
            case SMALLINT: {
                final short value = PgBinds.bindNonNullToShort(batchIndex, bindValue.getType(), bindValue);
                writeSafeString(message, Short.toString(value), 0);
            }
            break;
            case INTEGER: {
                final int value = PgBinds.bindNonNullToInt(batchIndex, bindValue.getType(), bindValue);
                writeSafeString(message, Integer.toString(value), 0);
            }
            break;
            case OID:
            case BIGINT: {
                final long value = PgBinds.bindNonNullToLong(batchIndex, bindValue.getType(), bindValue);
                writeSafeString(message, Long.toString(value), 0);
            }
            break;
            case DECIMAL: {
                final BigDecimal value = PgBinds.bindNonNullToDecimal(batchIndex, bindValue.getType(), bindValue);
                writeSafeString(message, value.toPlainString(), 0);
            }
            break;
            case REAL: {
                final float value = PgBinds.bindNonNullToFloat(batchIndex, bindValue.getType(), bindValue);
                writeSafeString(message, Float.toString(value), 0);
            }
            break;
            case DOUBLE: {
                final double value = PgBinds.bindNonNullToDouble(batchIndex, bindValue.getType(), bindValue);
                writeSafeString(message, Double.toString(value), 0);
            }
            break;
            case BOOLEAN: {
                final boolean value = PgBinds.bindNonNullToBoolean(batchIndex, bindValue.getType(), bindValue);
                writeSafeString(message, value ? PgConstant.TRUE : PgConstant.FALSE, 0);
            }
            break;
            case BYTEA: {
                bindNonNullToBytea(batchIndex, bindValue, message);
            }
            break;
            case VARCHAR:
            case MONEY:
            case TEXT:
            case JSON:
            case JSONB:
            case CHAR:
            case XML:
            case LINE:
            case UUID:
            case CIDR:
            case INET:
            case MACADDR:
            case MACADDR8:
            case PATH:
            case POINT:
            case CIRCLE:
            case BOX:
            case POLYGON:
            case LINE_SEGMENT: {
                bindNonNullToString(batchIndex, bindValue, message);
            }
            break;
            case BIT:
            case VARBIT: {
                bindNonNullToBit(batchIndex, bindValue, message);
            }
            break;
            case INTERVAL: {
                bindNonNullToDuration(batchIndex, bindValue, message);
            }
            break;
            case TIME: {
                bindNonNullToLocalTime(batchIndex, bindValue, message);
            }
            break;
            case TIMETZ: {
                bindNonNullToOffsetTime(batchIndex, bindValue, message);
            }
            break;
            case DATE: {
                bindNonNullToLocalDate(batchIndex, bindValue, message);
            }
            break;
            case TIMESTAMP: {
                bindNonNullToLocalDateTime(batchIndex, bindValue, message);
            }
            break;
            case TIMESTAMPTZ: {
                bindNonNullToOffsetDateTime(batchIndex, bindValue, message);
            }
            break;
            case TEXT_ARRAY:
            case BIT_ARRAY:
            case OID_ARRAY:
            case XML_ARRAY:
            case BOOLEAN_ARRAY:
            case CHAR_ARRAY:
            case DATE_ARRAY:
            case JSON_ARRAY:
            case TIME_ARRAY:
            case UUID_ARRAY:
            case BYTEA_ARRAY:
            case JSONB_ARRAY:
            case MONEY_ARRAY:
            case POINT_ARRAY:
            case REAL_ARRAY:
            case DOUBLE_ARRAY:
            case TIMETZ_ARRAY:
            case VARBIT_ARRAY:
            case DECIMAL_ARRAY:
            case INTEGER_ARRAY:
            case VARCHAR_ARRAY:
            case INTERVAL_ARRAY:
            case SMALLINT_ARRAY:
            case TIMESTAMP_ARRAY:
            case REF_CURSOR_ARRAY:
            case TIMESTAMPTZ_ARRAY: {
                throw new UnsupportedOperationException();
            }
            case REF_CURSOR:
            case UNSPECIFIED:
                throw PgExceptions.createNonSupportBindSqlTypeError(batchIndex, bindValue.getType(), bindValue);
            default:
                throw PgExceptions.createUnexpectedEnumException(bindValue.getType());

        }
    }


    private void writeSafeString(ByteBuf message, String text, final int suffixBytes) throws SQLException {
        final byte[] bytes = text.getBytes(this.clientCharset);
        if (message.maxWritableBytes() < (bytes.length + suffixBytes)) {
            throw PgExceptions.tooLargeObject();
        }
        message.writeBytes(bytes);
    }


    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToString(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException, IOException {
        final Object nonNull = bindValue.getNonNull();

        message.writeByte('E');
        message.writeByte(QUOTE_BYTE);
        if (nonNull instanceof String) {
            final byte[] bytes = ((String) nonNull).getBytes(this.clientCharset);
            writeCStyleEscape(message, bytes, bytes.length);
        } else if (nonNull instanceof byte[]) {
            final byte[] bytes = ((byte[]) nonNull);
            writeCStyleEscape(message, bytes, bytes.length);
        } else if (nonNull instanceof Enum) {
            message.writeBytes(((Enum<?>) nonNull).name().getBytes(this.clientCharset));
        } else if (nonNull instanceof UUID) {
            final byte[] bytes = nonNull.toString().getBytes(this.clientCharset);
            message.writeBytes(bytes);
        } else if (nonNull instanceof Path) {
            try (FileChannel channel = FileChannel.open((Path) nonNull, StandardOpenOption.READ)) {
                final byte[] bufferArray = new byte[2048];
                final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

                while (channel.read(buffer) > 0) {
                    buffer.flip();
                    writeCStyleEscape(message, bufferArray, buffer.remaining());
                    buffer.clear();
                }
            }
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToBytea(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException, IOException {
        final Object nonNull = bindValue.getNonNull();

        message.writeByte(QUOTE_BYTE);
        message.writeByte(BACK_SLASH_BYTE);
        message.writeByte('x');

        if (nonNull instanceof byte[]) {
            final byte[] bytes = ((byte[]) nonNull);
            if (message.maxWritableBytes() < bytes.length) {
                throw PgExceptions.tooLargeObject();
            }
            message.writeBytes(JdbdBuffers.hexEscapes(true, bytes, bytes.length));
        } else if (nonNull instanceof Path) {
            try (FileChannel channel = FileChannel.open((Path) nonNull, StandardOpenOption.READ)) {
                final byte[] bufferArray = new byte[2048];
                final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
                while (channel.read(buffer) > 0) {
                    buffer.flip();
                    message.writeBytes(JdbdBuffers.hexEscapes(true, bufferArray, buffer.remaining()));
                    buffer.clear();
                }
            }
        } else {
            throw PgExceptions.createNotSupportBindTypeError(batchIndex, bindValue);
        }
        message.writeByte(QUOTE_BYTE);

    }


    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalDate(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final LocalDate value;
        value = PgBinds.bindNonNullToLocalDate(batchIndex, bindValue.getType(), bindValue);

        message.writeByte(QUOTE_BYTE);
        try {
            message.writeBytes(value.format(PgTimes.PG_ISO_LOCAL_DATE_FORMATTER).getBytes(this.clientCharset));
        } catch (DateTimeException e) {
            throw PgExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
        }
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalTime(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final LocalTime value;
        value = PgBinds.bindNonNullToLocalTime(batchIndex, bindValue.getType(), bindValue);

        message.writeByte(QUOTE_BYTE);
        message.writeBytes(value.format(PgTimes.ISO_LOCAL_TIME_FORMATTER).getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToOffsetTime(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final OffsetTime value;
        value = PgBinds.bindNonNullToOffsetTime(batchIndex, bindValue.getType(), bindValue);
        message.writeByte(QUOTE_BYTE);
        message.writeBytes(value.format(PgTimes.ISO_OFFSET_TIME_FORMATTER).getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToOffsetDateTime(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final OffsetDateTime value;
        value = PgBinds.bindNonNullToOffsetDateTime(batchIndex, bindValue.getType(), bindValue);

        message.writeByte(QUOTE_BYTE);
        try {
            message.writeBytes(value.format(PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER).getBytes(this.clientCharset));
        } catch (DateTimeException e) {
            throw PgExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
        }
        message.writeByte(QUOTE_BYTE);
    }


    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalDateTime(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final LocalDateTime value;
        value = PgBinds.bindNonNullToLocalDateTime(batchIndex, bindValue.getType(), bindValue);

        message.writeByte(QUOTE_BYTE);
        try {
            message.writeBytes(value.format(PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER).getBytes(this.clientCharset));
        } catch (DateTimeException e) {
            throw PgExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
        }
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToBit(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final String bitString;
        bitString = PgBinds.bindNonNullToBit(stmtIndex, bindValue.getType(), bindValue);
        message.writeByte('B');
        message.writeByte(QUOTE_BYTE);
        message.writeBytes(bitString.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToDuration(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final String intervalString;
        intervalString = PgBinds.bindNonNullToInterval(stmtIndex, bindValue.getType(), bindValue);

        message.writeByte(QUOTE_BYTE);
        message.writeBytes(intervalString.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }


    /**
     * @see #bindNonNullToString(int, BindValue, ByteBuf)
     */
    private void writeCStyleEscape(ByteBuf message, final byte[] bytes, final int length) {
        if (length < 0 || length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                    "length[%s] and bytes.length[%s] not match.", length, bytes.length));
        }
        int lastWritten = 0;
        byte b;
        for (int i = 0; i < bytes.length; i++) {
            b = bytes[i];
            if (b == QUOTE_BYTE) {
                if (i > lastWritten) {
                    message.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                message.writeByte(QUOTE_BYTE);
                lastWritten = i;
            } else if (b == BACK_SLASH_BYTE) {
                if (i > lastWritten) {
                    message.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                message.writeByte(BACK_SLASH_BYTE);
                lastWritten = i;
            }

        }

        if (lastWritten < length) {
            message.writeBytes(bytes, lastWritten, length - lastWritten);
        }

    }


}
