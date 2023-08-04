package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.meta.BooleanMode;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.env.PgKey;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.*;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.util.JdbdNumbers;
import io.netty.buffer.ByteBuf;
import io.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.*;
import java.util.List;

/**
 * <p>
 * This class is writer of postgre simple query protocol.
 * </p>
 *
 * @see SimpleQueryTask
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class QueryCommandWriter {

    static Publisher<ByteBuf> staticCommand(final String sql, final TaskAdjutant adjutant) throws JdbdException {
        final byte[] sqlBytes;
        sqlBytes = sql.getBytes(adjutant.clientCharset());

        final int capacity = sqlBytes.length + 6;
        if (capacity < 0) {
            throw PgExceptions.createObjectTooLargeError();
        }
        final ByteBuf message;
        message = adjutant.allocator().buffer(capacity);

        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_BYTES); // placeholder
        message.writeBytes(sqlBytes);
        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return Mono.just(message);
    }


    static Publisher<ByteBuf> staticBatchCommand(final StaticBatchStmt stmt, final TaskAdjutant adjutant)
            throws JdbdException {
        final List<String> sqlGroup = stmt.getSqlGroup();
        final int groupSize = sqlGroup.size();
        final ByteBuf message = adjutant.allocator().buffer(groupSize * 50, Integer.MAX_VALUE);
        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_BYTES); // placeholder of length
        try {
            final Charset charset = adjutant.clientCharset();
            final byte[] semicolonBytes = PgConstant.SPACE_SEMICOLON_SPACE.getBytes(charset);
            String sql;
            for (int i = 0; i < groupSize; i++) {
                sql = sqlGroup.get(i);

                if (!adjutant.isSingleStmt(sql)) {
                    throw PgExceptions.createMultiStatementError();
                }
                if (i > 0) {
                    message.writeBytes(semicolonBytes);
                }
                message.writeBytes(sql.getBytes(charset));

            }

            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return Mono.just(message);
        } catch (Throwable e) {
            message.release();
            throw PgExceptions.wrapForMessage(e);
        }
    }


    static Publisher<ByteBuf> paramCommand(ParamStmt stmt, final TaskAdjutant adjutant) throws JdbdException {
        try {
            QueryCommandWriter writer = new QueryCommandWriter(adjutant);
            return Mono.just(writer.writeParamStmt(stmt));
        } catch (Throwable e) {
            throw PgExceptions.wrapForMessage(e);
        }
    }

    static Publisher<ByteBuf> paramBatchCommand(final ParamBatchStmt stmt, final TaskAdjutant adjutant)
            throws JdbdException {
        try {
            QueryCommandWriter writer = new QueryCommandWriter(adjutant);
            final ByteBuf message;
            message = writer.writeBatchBindCommand(stmt);
            return Mono.just(message);
        } catch (Throwable e) {
            throw PgExceptions.wrapForMessage(e);
        }
    }

    static Publisher<ByteBuf> multiStmtCommand(final ParamMultiStmt stmt, final TaskAdjutant adjutant)
            throws JdbdException {
        try {
            QueryCommandWriter writer = new QueryCommandWriter(adjutant);
            final ByteBuf message;
            message = writer.writeMultiBindCommand(stmt.getStmtList());
            return Mono.just(message);
        } catch (Throwable e) {
            throw PgExceptions.wrapForMessage(e);
        }
    }


    private final TaskAdjutant adjutant;

    private final Charset clientCharset;

    private final boolean hexEscapes;

    private final boolean clientUtf8;


    private QueryCommandWriter(final TaskAdjutant adjutant) {
        this.adjutant = adjutant;
        this.clientCharset = adjutant.clientCharset();
        this.clientUtf8 = this.clientCharset.equals(StandardCharsets.UTF_8);
        this.hexEscapes = adjutant.obtainHost().getProperties().getOrDefault(PgKey.hexEscapes, Boolean.class);
    }


    private ByteBuf writeParamStmt(final ParamStmt stmt) throws JdbdException {
        final String sql = stmt.getSql();
        final TaskAdjutant adjutant = this.adjutant;

        final PgStatement pgStmt;
        pgStmt = adjutant.parse(sql);
        final List<String> sqlPartList = pgStmt.sqlPartList();


        final ByteBuf message = adjutant.allocator()
                .buffer(sql.length() + (sqlPartList.size() * 10), Integer.MAX_VALUE);

        message.writeByte(Messages.Q);
        message.writeZero(Messages.LENGTH_BYTES); // placeholder

        writeStatement(-1, pgStmt, stmt.getBindGroup(), message);

        message.writeByte(Messages.STRING_TERMINATOR);

        Messages.writeLength(message);
        return message;
    }


    private ByteBuf writeMultiBindCommand(final List<ParamStmt> stmtList) throws JdbdException {
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
            ParamStmt stmt;
            final int stmtCount = stmtList.size();
            for (int i = 0; i < stmtCount; i++) {
                stmt = stmtList.get(i);
                statement = sqlParser.parse(stmt.getSql());
                if (i > 0) {
                    message.writeByte(PgConstant.SEMICOLON_BYTE);
                }
                writeStatement(i, statement, stmt.getBindGroup(), message);
            }

            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);

            return message;
        } catch (Throwable e) {
            message.release();
            throw e;
        }
    }

    /**
     * @see #createBindableBatchCommand(BindBatchStmt, TaskAdjutant)
     */
    private ByteBuf writeBatchBindCommand(final BindBatchStmt stmt) throws SQLException {
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
                    message.writeByte(PgConstant.SEMICOLON_BYTE);
                }
                writeStatement(i, statement, groupList.get(i), message);
            }

            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return message;
        } catch (Throwable e) {
            message.release();
            throw e;
        }
    }


    private void writeStatement(final int stmtIndex, final List<String> sqlPartList, final List<ParamValue> valueList,
                                final ByteBuf message) throws JdbdException {

        final int paramCount = sqlPartList.size() - 1;
        if (valueList.size() != paramCount) {
            throw PgExceptions.createBindCountNotMatchError(stmtIndex, paramCount, valueList.size());
        }
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = PgConstant.NULL.getBytes(clientCharset);
        ParamValue paramValue;
        Object value;
        DataType dataType;
        for (int i = 0; i < paramCount; i++) {
            paramValue = valueList.get(i);
            if (paramValue.getIndex() != i) {
                throw PgExceptions.createBindIndexNotMatchError(stmtIndex, i, paramValue);
            }
            message.writeBytes(sqlPartList.get(i).getBytes(clientCharset));
            value = paramValue.getValue();
            if (value == null) {
                message.writeBytes(nullBytes);
                continue;
            }
            dataType = paramValue.getType();
            if (dataType.isArray()) {
                if (value instanceof String || dataType.isUserDefined() == BooleanMode.TRUE) {
                    bindStringToArray(stmtIndex, paramValue, message);
                } else {
                    bindArrayObject(stmtIndex, paramValue, message);
                }
            } else if (dataType instanceof PgType) {
                bindBuildInType(stmtIndex, paramValue, message);
            } else if (value instanceof String) {
                writeBackslashEscapes((String) value, message);
            } else {
                throw PgExceptions.nonSupportBindSqlTypeError(stmtIndex, paramValue);
            }

        }

        message.writeBytes(sqlPartList.get(paramCount).getBytes(clientCharset));

    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT">Numeric Types</a>
     */
    private void bindBuildInType(final int batchIndex, final ParamValue bindValue, final ByteBuf message)
            throws JdbdException {
        final Charset clientCharset = this.clientCharset;
        final PgType pgType = (PgType) bindValue.getType();
        switch (pgType) {
            case SMALLINT: {
                final int value = PgBinds.bindToInt(batchIndex, bindValue, Short.MIN_VALUE, Short.MAX_VALUE);
                message.writeBytes(Integer.toString(value).getBytes(clientCharset));
                message.writeBytes("::SMALLINT".getBytes(clientCharset));
            }
            break;
            case INTEGER: {
                final int value = PgBinds.bindToInt(batchIndex, bindValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
                message.writeBytes(Integer.toString(value).getBytes(clientCharset));
                message.writeBytes("::INTEGER".getBytes(clientCharset));
            }
            break;
            case OID:
            case BIGINT: {
                final long value = PgBinds.bindToLong(batchIndex, bindValue, Long.MIN_VALUE, Long.MAX_VALUE);
                message.writeBytes(Long.toString(value).getBytes(clientCharset));
                message.writeBytes("::BIGINT".getBytes(clientCharset));
            }
            break;
            case DECIMAL: {
                final BigDecimal value = PgBinds.bindToDecimal(batchIndex, bindValue);
                message.writeBytes(value.toPlainString().getBytes(clientCharset));
                message.writeBytes("::DECIMAL".getBytes(clientCharset));
            }
            break;
            case REAL: {
                final float value = PgBinds.bindToFloat(batchIndex, bindValue);
                message.writeBytes(Float.toString(value).getBytes(clientCharset));
                message.writeBytes("::REAL".getBytes(clientCharset));
            }
            break;
            case FLOAT8: {
                final double value = PgBinds.bindToDouble(batchIndex, bindValue);
                message.writeBytes(Double.toString(value).getBytes(clientCharset));
                message.writeBytes("::FLOAT8".getBytes(clientCharset));
            }
            break;
            case BOOLEAN: {
                final boolean value = PgBinds.bindToBoolean(batchIndex, bindValue);
                message.writeBytes((value ? PgConstant.TRUE : PgConstant.FALSE).getBytes(this.clientCharset));
            }
            break;
            case BYTEA:
                bindToBytea(batchIndex, bindValue, message);
                break;
            case CHAR:
            case VARCHAR:
            case TEXT:
            case TSQUERY:
            case TSVECTOR:

            case INT4RANGE:
            case INT8RANGE:
            case NUMRANGE:
            case DATERANGE:
            case TSRANGE:
            case TSTZRANGE:

            case INT4MULTIRANGE:
            case INT8MULTIRANGE:
            case NUMMULTIRANGE:
            case DATEMULTIRANGE:
            case TSMULTIRANGE:
            case TSTZMULTIRANGE:


            case JSON:
            case JSONB:
            case XML:

            case POINT:
            case LINE:
            case PATH:
            case CIRCLE:
            case BOX:
            case POLYGON:
            case LSEG:

            case CIDR:
            case INET:
            case MACADDR:
            case MACADDR8:

            case UUID: {
                message.writeBytes(pgType.name().getBytes(clientCharset));
                message.writeByte(PgConstant.SPACE);
                writeString(batchIndex, bindValue, message);
            }
            break;
            case BIT:
            case VARBIT: {
                final String bitString;
                bitString = PgBinds.bindToBit(batchIndex, bindValue, Integer.MAX_VALUE);

                message.writeBytes(pgType.name().getBytes(clientCharset));
                message.writeByte(PgConstant.SPACE);

                message.writeByte('B');
                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(bitString.getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case INTERVAL: {
                final String intervalString;
                intervalString = PgBinds.bindToInterval(batchIndex, bindValue);

                message.writeBytes(pgType.name().getBytes(clientCharset));
                message.writeByte(PgConstant.SPACE);

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(intervalString.getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case TIME: {
                final LocalTime value;
                value = PgBinds.bindToLocalTime(batchIndex, bindValue);

                message.writeBytes("TIME ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.format(PgTimes.TIME_FORMATTER_6).getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);

            }
            break;
            case TIMETZ: {
                final OffsetTime value;
                value = PgBinds.bindToOffsetTime(batchIndex, bindValue);

                message.writeBytes("TIMETZ ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.format(PgTimes.OFFSET_TIME_FORMATTER_6).getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case DATE: {
                final LocalDate value;
                value = PgBinds.bindToLocalDate(batchIndex, bindValue);

                message.writeBytes("DATE ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.toString().getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case TIMESTAMP: {
                final LocalDateTime value;
                value = PgBinds.bindToLocalDateTime(batchIndex, bindValue);

                message.writeBytes("TIMESTAMP ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.format(PgTimes.DATETIME_FORMATTER_6).getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case TIMESTAMPTZ: {
                final OffsetDateTime value;
                value = PgBinds.bindToOffsetDateTime(batchIndex, bindValue);

                message.writeBytes("TIMESTAMPTZ ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.format(PgTimes.OFFSET_DATETIME_FORMATTER_6).getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case MONEY:
                writeMoney(batchIndex, bindValue, message);
                break;
            case REF_CURSOR:
            case UNSPECIFIED:
            default:
                throw PgExceptions.unexpectedEnum(pgType);

        }
    }


    /**
     * @see #writeStatement(int, List, List, ByteBuf)
     */
    private void bindStringToArray(final int batchIndex, final ParamValue paramValue, final ByteBuf message) {
        final DataType dataType = paramValue.getType();

        if (dataType == PgType.REF_CURSOR_ARRAY) {
            throw PgExceptions.unexpectedEnum((PgType) dataType);
        } else if (!dataType.isArray()) {
            throw new JdbdException(String.format("unexpected %s[%s]", DataType.class.getName(), dataType));
        }

        final String arrayValue = (String) paramValue.getNonNullValue();
        if (arrayValue.charAt(0) != PgConstant.LEFT_BRACKET
                || arrayValue.charAt(arrayValue.length() - 1) != PgConstant.RIGHT_BRACKET) {
            throw PgExceptions.outOfTypeRange(batchIndex, paramValue);
        }

        final String typeSuffix;
        typeSuffix = dataType.typeName();
        if (!(dataType instanceof PgType) && !typeSuffix.endsWith("[]")) {
            throw PgExceptions.errorTypeName(dataType);
        }

        writeBackslashEscapes(arrayValue, message);

        final Charset clientCharset = this.clientCharset;

        message.writeBytes("::".getBytes(clientCharset));
        message.writeBytes(typeSuffix.getBytes(clientCharset));

    }

    /**
     * @see #writeStatement(int, List, List, ByteBuf)
     */
    private void bindArrayObject(final int batchIndex, final ParamValue paramValue, final ByteBuf message)
            throws JdbdException {
        final Object value = paramValue.getNonNullValue();
        final Charset clientCharset = this.clientCharset;
        final String v;
        final PgType pgType = (PgType) paramValue.getType();

        switch (pgType) {
            case BOOLEAN_ARRAY:
            case SMALLINT_ARRAY:
            case INTEGER_ARRAY:
            case BIGINT_ARRAY:
            case DECIMAL_ARRAY:
            case REAL_ARRAY:
            case DOUBLE_ARRAY:

            case OID_ARRAY:
            case MONEY_ARRAY:

            case TIME_ARRAY:
            case TIMETZ_ARRAY:
            case DATE_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
            case INTERVAL_ARRAY:

            case BYTEA_ARRAY:

            case BIT_ARRAY:
            case VARBIT_ARRAY:

            case UUID_ARRAY:

            case CHAR_ARRAY:
            case VARCHAR_ARRAY:
            case TEXT_ARRAY:
            case JSON_ARRAY:
            case JSONB_ARRAY:
            case XML_ARRAY:
            case TSQUERY_ARRAY:
            case TSVECTOR_ARRAY:

            case INT4RANGE_ARRAY:
            case INT8RANGE_ARRAY:
            case NUMRANGE_ARRAY:
            case DATERANGE_ARRAY:
            case TSRANGE_ARRAY:
            case TSTZRANGE_ARRAY:

            case INT4MULTIRANGE_ARRAY:
            case INT8MULTIRANGE_ARRAY:
            case NUMMULTIRANGE_ARRAY:
            case DATEMULTIRANGE_ARRAY:
            case TSMULTIRANGE_ARRAY:
            case TSTZMULTIRANGE_ARRAY:

            case POINT_ARRAY:
            case LINE_ARRAY:
            case PATH_ARRAY:
            case BOX_ARRAY:
            case LSEG_ARRAY:
            case CIRCLE_ARRAY:
            case POLYGON_ARRAY:

            case CIDR_ARRAY:
            case INET_ARRAY:
            case MACADDR_ARRAY:
            case MACADDR8_ARRAY:

            default:
                throw PgExceptions.unexpectedEnum(pgType);
        }
        message.writeByte('E');
        message.writeByte(PgConstant.QUOTE);
        final byte[] bytes = v.getBytes(this.clientCharset);
        writeWithEscape(message, bytes, bytes.length);
        message.writeByte(PgConstant.QUOTE);

        if (paramValue.getType() == PgType.MONEY_ARRAY) {
            // decimal array must append type converting or result error.
            final Pair<Class<?>, Integer> pair = PgArrays.getArrayDimensions(paramValue.getNonNull().getClass());
            if (pair.getFirst() == BigDecimal.class) {
                final int dimension = pair.getSecond();
                final StringBuilder builder = new StringBuilder(16 + (dimension << 2));
                builder.append("::decimal");
                for (int i = 0; i < dimension; i++) {
                    builder.append("[]");
                }
                builder.append("::money");
                for (int i = 0; i < dimension; i++) {
                    builder.append("[]");
                }
                message.writeBytes(builder.toString().getBytes(this.clientCharset));
            }
        }

    }


    /**
     * @see #bindBuildInType(int, ParamValue, ByteBuf)
     */
    private void writeString(final int batchIndex, ParamValue bindValue, ByteBuf message)
            throws JdbdException {
        final String value;
        value = PgBinds.bindToString(batchIndex, bindValue);


    }

    /**
     * @see #bindBuildInType(int, ParamValue, ByteBuf)
     * @see PgType#MONEY
     */
    private void writeMoney(final int batchIndex, final ParamValue bindValue, final ByteBuf message)
            throws JdbdException {
        final Object value = bindValue.getValue();


        if (value instanceof Number || (value instanceof String && JdbdNumbers.isDecimal((String) value))) {
            final byte[] bytes;
            if (value instanceof BigDecimal) {
                bytes = ((BigDecimal) value).toPlainString().getBytes(this.clientCharset);
            } else if (value instanceof Long
                    || value instanceof Integer
                    || value instanceof Short
                    || value instanceof Byte
                    || value instanceof BigInteger) {
                // not support double and float
                bytes = value.toString().getBytes(this.clientCharset);
            } else if (value instanceof String) {
                bytes = ((String) value).getBytes(this.clientCharset);
            } else {
                throw PgExceptions.createNotSupportBindTypeError(batchIndex, bindValue);
            }
            message.writeByte(PgConstant.QUOTE);
            message.writeBytes(bytes);
            message.writeByte(PgConstant.QUOTE);
            message.writeBytes("::DECIMAL::MONEY".getBytes(this.clientCharset));
        } else if (value instanceof String) {
            message.writeBytes("MONEY ".getBytes(this.clientCharset));
            writeBackslashEscapes((String) value, message);
        } else {
            throw PgExceptions.createNotSupportBindTypeError(batchIndex, bindValue);
        }


    }


    /**
     * @see #bindBuildInType(int, ParamValue, ByteBuf)
     * @see <a href="https://www.postgresql.org/docs/current/datatype-binary.html">Binary Data Types</a>
     */
    private void bindToBytea(final int batchIndex, final ParamValue bindValue, final ByteBuf message) {
        final Object value;
        value = bindValue.getValue();
        if (!(value instanceof byte[])) {
            throw PgExceptions.createNotSupportBindTypeError(batchIndex, bindValue);
        }
        final byte[] v = (byte[]) value;

        message.writeBytes("BYTEA ".getBytes(this.clientCharset));
        message.writeByte(PgConstant.QUOTE);
        message.writeByte(PgConstant.BACK_SLASH_BYTE);
        message.writeByte('x');
        message.writeBytes(PgBuffers.hexEscapes(true, v, v.length));
        message.writeByte(PgConstant.QUOTE);

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS">String Constants</a>
     */
    private void writeBackslashEscapes(final String value, final ByteBuf message) {
        final byte[] bytes;
        bytes = value.getBytes(this.clientCharset);
        final int length, startIndex;
        length = bytes.length;
        startIndex = message.writerIndex();

        message.writeByte(PgConstant.SPACE); // placeholder for 'E'
        message.writeByte(PgConstant.QUOTE);

        int lastWritten = 0;
        char followChar = PgConstant.NUL;
        for (int i = 0; i < length; i++) {
            switch (bytes[i]) {
                case PgConstant.QUOTE: {
                    if (i > lastWritten) {
                        message.writeBytes(bytes, lastWritten, i - lastWritten);
                    }
                    message.writeByte(PgConstant.QUOTE);  // because jdbd-postgre support only the charset that ASCII is one byte
                    lastWritten = i;//not i + 1 as current char wasn't written
                }
                continue;
                case PgConstant.BACK_SLASH:
                    followChar = PgConstant.BACK_SLASH;
                    break;
                case PgConstant.NUL:
                    followChar = '0';
                    break;
                case '\b':
                    followChar = 'b';
                    break;
                case '\f':
                    followChar = 'f';
                    break;
                case '\n':
                    followChar = 'n';
                    break;
                case '\r':
                    followChar = 'r';
                    break;
                case '\t':
                    followChar = 't';
                    break;
                default:
                    continue;
            }

            if (i > lastWritten) {
                message.writeBytes(bytes, lastWritten, i - lastWritten);
            }
            message.writeByte(PgConstant.BACK_SLASH);  // because jdbd-postgre support only the charset that ASCII is one byte
            message.writeByte(followChar);
            lastWritten = i + 1;


        }// for

        if (lastWritten < length) {
            message.writeBytes(bytes, lastWritten, length - lastWritten);
        }

        if (followChar != PgConstant.NUL) {
            message.setByte(startIndex, 'E');
        }

    }

    /**
     * @see #bindToBytea(int, BindValue, ByteBuf)
     */
    private void writeBinaryPathWithEscapes(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws LongDataReadException {
        final Path path = (Path) bindValue.getNonNull();

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {

            message.writeByte(PgConstant.QUOTE);
            message.writeByte(PgConstant.BACK_SLASH_BYTE);
            message.writeByte('x');

            final byte[] bufferArray = new byte[2048];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
            while (channel.read(buffer) > 0) {
                buffer.flip();
                message.writeBytes(PgBuffers.hexEscapes(true, bufferArray, buffer.remaining()));
                buffer.clear();
            }
            message.writeByte(PgConstant.QUOTE);
        } catch (Throwable e) {
            String msg = String.format("batch[%s] parameter[%s] %s read occur error."
                    , batchIndex, bindValue.getIndex(), path);
            throw new LongDataReadException(msg, e);
        }

    }


    /**
     * @see #bindParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalDate(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        if (bindValue.getNonNull() instanceof String
                && tryWriteDateOrTimestampSpecialValue(bindValue, message)) {
            return;
        }

        final LocalDate value;
        value = PgBinds.bindToLocalDate(batchIndex, bindValue.getType(), bindValue);
        try {
            message.writeByte(PgConstant.QUOTE);
            message.writeBytes(value.format(PgTimes.PG_ISO_LOCAL_DATE_FORMATTER).getBytes(this.clientCharset));
            message.writeByte(PgConstant.QUOTE);
        } catch (DateTimeException e) {
            throw PgExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
        }

    }


    /**
     * @see #bindParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToOffsetTime(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        final OffsetTime value;
        value = PgBinds.bindToOffsetTime(batchIndex, bindValue.getType(), bindValue);
        message.writeByte(PgConstant.QUOTE);
        message.writeBytes(value.format(PgTimes.ISO_OFFSET_TIME_FORMATTER).getBytes(this.clientCharset));
        message.writeByte(PgConstant.QUOTE);
    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToOffsetDateTime(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {

        if (bindValue.getNonNull() instanceof String
                && tryWriteDateOrTimestampSpecialValue(bindValue, message)) {
            return;
        }

        final OffsetDateTime value;
        value = PgBinds.bindToOffsetDateTime(batchIndex, bindValue.getType(), bindValue);
        try {
            message.writeByte(PgConstant.QUOTE);
            message.writeBytes(value.format(PgTimes.PG_ISO_OFFSET_DATETIME_FORMATTER).getBytes(this.clientCharset));
            message.writeByte(PgConstant.QUOTE);
        } catch (DateTimeException e) {
            throw PgExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
        }

    }


    /**
     * @see #bindParameter(int, BindValue, ByteBuf)
     */
    private void bindNonNullToLocalDateTime(final int batchIndex, BindValue bindValue, ByteBuf message)
            throws SQLException {
        if (bindValue.getNonNull() instanceof String
                && tryWriteDateOrTimestampSpecialValue(bindValue, message)) {
            return;
        }

        final LocalDateTime value;
        value = PgBinds.bindToLocalDateTime(batchIndex, bindValue.getType(), bindValue);

        message.writeByte(PgConstant.QUOTE);
        try {
            message.writeBytes(value.format(PgTimes.PG_ISO_LOCAL_DATETIME_FORMATTER).getBytes(this.clientCharset));
        } catch (DateTimeException e) {
            throw PgExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
        }
        message.writeByte(PgConstant.QUOTE);
    }

    /**
     * @return true : bindValue is special value and write complete.
     */
    private boolean tryWriteDateOrTimestampSpecialValue(BindValue bindValue, ByteBuf message) {
        switch (bindValue.getType()) {
            case TIMESTAMP:
            case DATE:
            case TIMESTAMPTZ:
//            case TIMESTAMP_ARRAY:
//            case DATE_ARRAY:
//            case TIMESTAMPTZ_ARRAY:
                break;
            default:
                throw new IllegalArgumentException("bind value error");
        }
        final String textValue = ((String) bindValue.getNonNull()).toLowerCase();
        switch (textValue) {
            case PgConstant.INFINITY:
            case PgConstant.NEG_INFINITY: {
                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(textValue.getBytes(this.clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            return true; // write complete
            default:
        }
        return false;
    }


    /**
     * @see #bindNonNullToString(int, BindValue, ByteBuf)
     */
    private void writeTextPathWithEscapes(final int batchIndex, BindValue bindValue, ByteBuf message) {
        final Path path = (Path) bindValue.getNonNull();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            message.writeByte('E');
            message.writeByte(PgConstant.QUOTE);

            final byte[] bufferArray = new byte[2048];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            final CharsetDecoder decoder;
            final CharsetEncoder encoder;

            if (this.clientUtf8) {
                decoder = null;
                encoder = null;
            } else {
                decoder = StandardCharsets.UTF_8.newDecoder();
                encoder = this.clientCharset.newEncoder();
            }
            while (channel.read(buffer) > 0) {
                buffer.flip();
                if (decoder == null) {
                    writeWithEscape(message, bufferArray, buffer.remaining());
                } else {
                    final ByteBuffer bf = encoder.encode(decoder.decode(buffer));
                    final byte[] encodedBytes;
                    final int length = bf.remaining();
                    if (bf.hasArray()) {
                        encodedBytes = bf.array();
                    } else {
                        encodedBytes = new byte[length];
                        bf.get(encodedBytes);
                    }
                    writeWithEscape(message, encodedBytes, length);
                }
                buffer.clear();
            }

            message.writeByte(PgConstant.QUOTE);
        } catch (Throwable e) {
            String msg = String.format("batch[%s] parameter[%s] read text path[%s] occur error."
                    , batchIndex, bindValue.getIndex(), path);
            throw new LongDataReadException(msg, e);
        }

    }


    /**
     * @see #bindNonNullToString(int, BindValue, ByteBuf)
     * @see #bindToBytea(int, BindValue, ByteBuf)
     * @see #writeBinaryPathWithEscapes(int, BindValue, ByteBuf)
     */
    private void writeWithEscape(ByteBuf message, final byte[] bytes, final int length) {
        if (length < 0 || length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                    "length[%s] and bytes.length[%s] not match.", length, bytes.length));
        }
        int lastWritten = 0;
        byte b;
        for (int i = 0; i < length; i++) {
            b = bytes[i];
            if (b == PgConstant.QUOTE) {
                if (i > lastWritten) {
                    message.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                message.writeByte(PgConstant.QUOTE);
                lastWritten = i;
            } else if (b == PgConstant.BACK_SLASH_BYTE) {
                if (i > lastWritten) {
                    message.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                message.writeByte(PgConstant.BACK_SLASH_BYTE);
                lastWritten = i;
            }

        }

        if (lastWritten < length) {
            message.writeBytes(bytes, lastWritten, length - lastWritten);
        }

    }


}
