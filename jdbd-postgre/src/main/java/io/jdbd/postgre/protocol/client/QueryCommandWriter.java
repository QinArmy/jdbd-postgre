package io.jdbd.postgre.protocol.client;

import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.stmt.BindableStmt;
import io.jdbd.postgre.stmt.MultiBindStmt;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.postgre.util.PgTimes;
import io.jdbd.vendor.stmt.GroupStmt;
import io.jdbd.vendor.stmt.Stmt;
import io.jdbd.vendor.syntax.SQLParser;
import io.jdbd.vendor.util.JdbdBuffers;
import io.jdbd.vendor.util.JdbdExceptions;
import io.jdbd.vendor.util.JdbdTimes;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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
        ByteBuf message;
        message = new QueryCommandWriter(adjutant)
                .writeCommand(Collections.singletonList(stmt));
        return Mono.just(message);
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

    private static final Logger LOG = LoggerFactory.getLogger(QueryCommandWriter.class);

    private static final String NULL = "NULL", TRUE = "TRUE", FALSE = "FALSE", B = "B";

    private static final byte BACK_SLASH_BYTE = '\\';

    private static final byte QUOTE_BYTE = '\'';

    private static final byte SEMICOLON_BYTE = ';';

    private final TaskAdjutant adjutant;

    private final Charset clientCharset;

    private QueryCommandWriter(TaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.clientCharset = adjutant.clientCharset();
    }


    private ByteBuf writeCommand(final List<BindableStmt> stmtList) throws Throwable {
        final TaskAdjutant adjutant = this.adjutant;
        final ByteBuf message = adjutant.allocator().buffer(stmtList.size() << 7, Integer.MAX_VALUE);

        try {
            message.writeByte(Messages.Q);
            message.writeZero(Messages.LENGTH_SIZE); // placeholder

            final PgParser sqlParser = adjutant.sqlParser();
            PgStatement statement;
            BindableStmt stmt;
            final int stmtCount = stmtList.size();
            for (int i = 0; i < stmtCount; i++) {
                stmt = stmtList.get(i);
                statement = sqlParser.parse(stmt.getSql());
                if (i > 0) {
                    message.writeByte(SEMICOLON_BYTE);
                }
                writeStatement(i, statement, stmt.getParamGroup(), message);
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
     * @see #writeCommand(List)
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
            if (bindValue.getParamIndex() != i) {
                throw PgExceptions.createBindIndexNotMatchError(stmtIndex, i, bindValue);
            }
            message.writeBytes(staticSqlList.get(i).getBytes(clientCharset));
            if (bindValue.getValue() == null) {
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
    private void bindNonNullParameter(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException, IOException {

        switch (bindValue.getType()) {
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case DOUBLE:
            case OID: {
                bindNonNullToNumber(stmtIndex, bindValue, message);
            }
            break;
            case BOOLEAN: {
                bindNonNullToBoolean(stmtIndex, bindValue, message);
            }
            break;
            case BYTEA: {
                bindNonNullToBytea(stmtIndex, bindValue, message);
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
                bindNonNullToString(stmtIndex, bindValue, message);
            }
            break;
            case BIT:
            case VARBIT: {
                bindNonNullToBit(stmtIndex, bindValue, message);
            }
            break;
            case INTERVAL: {
                bindNonNullToDuration(stmtIndex, bindValue, message);
            }
            break;
            case TIME: {
                bindNonNullToLocalTime(stmtIndex, bindValue, message);
            }
            break;
            case TIMETZ: {
                bindNonNullToOffsetTime(stmtIndex, bindValue, message);
            }
            break;
            case DATE: {
                bindNonNullToLocalDate(stmtIndex, bindValue, message);
            }
            break;
            case TIMESTAMP: {
                bindNonNullToLocalDateTime(stmtIndex, bindValue, message);
            }
            break;
            case TIMESTAMPTZ: {
                bindNonNullToOffsetDateTime(stmtIndex, bindValue, message);
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
                throw PgExceptions.createNonSupportBindSqlTypeError(stmtIndex, bindValue);
            default:
                throw PgExceptions.createUnknownEnumException(bindValue.getType());

        }
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToNumber(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String numberText;
        if (nonNull instanceof Number) {
            if (nonNull instanceof BigDecimal) {
                numberText = ((BigDecimal) nonNull).toPlainString();
            } else if (nonNull instanceof Integer
                    || nonNull instanceof Long
                    || nonNull instanceof BigInteger
                    || nonNull instanceof Float
                    || nonNull instanceof Double
                    || nonNull instanceof Short
                    || nonNull instanceof Byte) {
                numberText = nonNull.toString();
            } else {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
        } else if (nonNull instanceof String) {
            if (PgStrings.isNumber((String) nonNull)) {
                numberText = (String) nonNull;
            } else {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
        } else if (nonNull instanceof Boolean) {
            final boolean boolValue = (Boolean) nonNull;
            numberText = boolValue ? "1" : "0";
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }

        message.writeBytes(numberText.getBytes(this.clientCharset));

    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToBoolean(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String boolText;

        if (nonNull instanceof Boolean) {
            boolText = ((Boolean) nonNull) ? TRUE : FALSE;
        } else if (nonNull instanceof String) {
            if (TRUE.equalsIgnoreCase((String) nonNull)) {
                boolText = TRUE;
            } else if (FALSE.equalsIgnoreCase((String) nonNull)) {
                boolText = FALSE;
            } else {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
        } else if (nonNull instanceof Number) {
            if (nonNull instanceof Integer
                    || nonNull instanceof Long
                    || nonNull instanceof Short
                    || nonNull instanceof Byte) {
                boolText = ((Number) nonNull).longValue() == 0L ? FALSE : TRUE;
            } else if (nonNull instanceof BigDecimal) {
                boolText = BigDecimal.ZERO.compareTo((BigDecimal) nonNull) == 0 ? FALSE : TRUE;
            } else if (nonNull instanceof BigInteger) {
                boolText = BigInteger.ZERO.compareTo((BigInteger) nonNull) == 0 ? FALSE : TRUE;
            } else if (nonNull instanceof Double
                    || nonNull instanceof Float) {
                boolText = ((Number) nonNull).doubleValue() == 0.0 ? FALSE : TRUE;
            } else {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }

        message.writeBytes(boolText.getBytes(this.clientCharset));
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToString(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException, IOException {
        final Object nonNull = bindValue.getNonNullValue();

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
    private void bindNonNullToBytea(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException, IOException {
        final Object nonNull = bindValue.getNonNullValue();

        message.writeByte(QUOTE_BYTE);
        message.writeByte(BACK_SLASH_BYTE);
        message.writeByte('x');

        if (nonNull instanceof byte[]) {
            final byte[] bytes = ((byte[]) nonNull);
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
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }
        message.writeByte(QUOTE_BYTE);

    }


    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalDate(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String dateText;

        if (nonNull instanceof LocalDate) {
            dateText = ((LocalDate) nonNull).format(DateTimeFormatter.ISO_LOCAL_DATE);
        } else if (nonNull instanceof String) {
            final String textValue = (String) nonNull;
            try {
                LocalDate.parse(textValue);
            } catch (DateTimeException e) {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
            dateText = textValue;
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }
        message.writeByte(QUOTE_BYTE);
        message.writeBytes(dateText.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalTime(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String timeText;

        if (nonNull instanceof LocalTime) {
            timeText = ((LocalTime) nonNull).format(JdbdTimes.ISO_LOCAL_TIME_FORMATTER);
        } else if (nonNull instanceof String) {
            final String textValue = (String) nonNull;
            try {
                LocalTime.parse(textValue, JdbdTimes.ISO_LOCAL_TIME_FORMATTER);
            } catch (DateTimeException e) {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
            timeText = textValue;
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }

        message.writeByte(QUOTE_BYTE);
        message.writeBytes(timeText.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToOffsetTime(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String timeText;

        if (nonNull instanceof OffsetTime) {
            timeText = ((OffsetTime) nonNull).format(PgTimes.ISO_OFFSET_TIME_FORMATTER);
        } else if (nonNull instanceof String) {
            final String textValue = (String) nonNull;
            try {
                OffsetTime.parse(textValue, PgTimes.ISO_OFFSET_TIME_FORMATTER);
            } catch (DateTimeException e) {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
            timeText = textValue;
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }

        message.writeByte(QUOTE_BYTE);
        message.writeBytes(timeText.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToOffsetDateTime(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String dateTimeText;
        if (nonNull instanceof OffsetDateTime
                || nonNull instanceof ZonedDateTime) {
            dateTimeText = PgTimes.ISO_OFFSET_DATETIME_FORMATTER.format((TemporalAccessor) nonNull);
        } else if (nonNull instanceof String) {
            final String textValue = (String) nonNull;
            try {
                OffsetDateTime.parse(textValue, PgTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } catch (DateTimeException e) {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
            dateTimeText = textValue;
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }

        message.writeByte(QUOTE_BYTE);
        message.writeBytes(dateTimeText.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }


    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalDateTime(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String dateTimeText;

        if (nonNull instanceof LocalDateTime) {
            dateTimeText = PgTimes.ISO_LOCAL_DATETIME_FORMATTER.format((LocalDateTime) nonNull);
        } else if (nonNull instanceof String) {
            final String textValue = (String) nonNull;
            try {
                LocalDateTime.parse(textValue, PgTimes.ISO_LOCAL_DATETIME_FORMATTER);
            } catch (DateTimeException e) {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
            dateTimeText = textValue;
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }

        message.writeByte(QUOTE_BYTE);
        message.writeBytes(dateTimeText.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToBit(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String bitString;
        if (nonNull instanceof Long) {
            bitString = PgStrings.reverse(Long.toBinaryString((Long) nonNull));
        } else if (nonNull instanceof BitSet) {
            bitString = PgStrings.bitSetToBitString((BitSet) nonNull, false);
        } else if (nonNull instanceof String) {
            final String textValue = (String) nonNull;
            if (!PgStrings.isBinaryString(textValue)) {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
            bitString = textValue;
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }

        message.writeBytes(B.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
        message.writeBytes(bitString.getBytes(this.clientCharset));
        message.writeByte(QUOTE_BYTE);
    }

    /**
     * @see #bindNonNullParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToDuration(final int stmtIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        final String intervalString;
        if (nonNull instanceof Duration) {
            intervalString = nonNull.toString();
        } else if (nonNull instanceof Period) {
            intervalString = nonNull.toString();
        } else if (nonNull instanceof String) {
            final String textValue = (String) nonNull;
            if (!PgStrings.isSafePgString(textValue)) {
                throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
            }
            intervalString = textValue;
        } else {
            throw PgExceptions.createNotSupportBindTypeError(stmtIndex, bindValue);
        }
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
