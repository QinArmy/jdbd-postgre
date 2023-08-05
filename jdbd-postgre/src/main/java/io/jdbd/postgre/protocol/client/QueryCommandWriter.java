package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.syntax.PgParser;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.*;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.util.JdbdNumbers;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * <p>
 * This class is writer of postgre simple query protocol.
 * </p>
 *
 * @see SimpleQueryTask
 * @see <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">Query</a>
 */
final class QueryCommandWriter extends CommandWriter {

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
        message.writeZero(Messages.LENGTH_SIZE); // placeholder
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
        message.writeZero(Messages.LENGTH_SIZE); // placeholder of length
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
            message = writer.writeParamBatchCommand(stmt);
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


    private static final Map<String, Boolean> KEY_WORD_MAP = createKeyWordMap();


    private QueryCommandWriter(final TaskAdjutant adjutant) {
        super(adjutant);
    }


    private ByteBuf writeParamStmt(final ParamStmt stmt) throws JdbdException {
        final String sql = stmt.getSql();
        final TaskAdjutant adjutant = this.adjutant;

        final PgStatement pgStmt;
        pgStmt = adjutant.parse(sql);
        final List<String> sqlPartList = pgStmt.sqlPartList();

        int capacity = sql.length() + (sqlPartList.size() << 3);
        if (capacity < 0) {
            capacity = Integer.MAX_VALUE - 128;
        }
        final ByteBuf message = adjutant.allocator().buffer(capacity, Integer.MAX_VALUE);

        try {
            message.writeByte(Messages.Q);
            message.writeZero(Messages.LENGTH_SIZE); // placeholder

            writeStatement(-1, sqlPartList, stmt.getBindGroup(), message);

            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return message;
        } catch (Throwable e) {
            message.release();
            throw PgExceptions.wrapForMessage(e);
        }
    }


    private ByteBuf writeMultiBindCommand(final List<ParamStmt> stmtList) throws JdbdException {
        final TaskAdjutant adjutant = this.adjutant;
        int capacity = stmtList.size() << 7;
        if (capacity < 0) {
            capacity = Integer.MAX_VALUE - 128;
        }
        final ByteBuf message = adjutant.allocator().buffer(capacity, Integer.MAX_VALUE);

        try {
            message.writeByte(Messages.Q);
            message.writeZero(Messages.LENGTH_SIZE); // placeholder

            final PgParser sqlParser = adjutant.sqlParser();
            PgStatement statement;
            ParamStmt stmt;
            final int stmtCount = stmtList.size();
            for (int i = 0; i < stmtCount; i++) {
                stmt = stmtList.get(i);
                statement = sqlParser.parse(stmt.getSql());
                if (i > 0) {
                    message.writeByte(PgConstant.SPACE); // because jdbd-postgre support only the charset that ASCII is one byte
                    message.writeByte(PgConstant.SEMICOLON);
                    message.writeByte(PgConstant.SPACE);
                }
                writeStatement(i, statement.sqlPartList(), stmt.getBindGroup(), message);
            }

            message.writeByte(Messages.STRING_TERMINATOR);

            Messages.writeLength(message);
            return message;
        } catch (Throwable e) {
            message.release();
            throw PgExceptions.wrapForMessage(e);
        }
    }


    private ByteBuf writeParamBatchCommand(final ParamBatchStmt stmt) throws JdbdException {
        final TaskAdjutant adjutant = this.adjutant;
        final String sql = stmt.getSql();
        final List<List<ParamValue>> groupList = stmt.getGroupList();
        final int stmtCount = groupList.size();

        int capacity = (sql.length() + 40) * stmtCount;
        if (capacity < 0) {
            capacity = Integer.MAX_VALUE - 128;
        }

        final List<String> sqlPartList;
        sqlPartList = adjutant.parse(sql).sqlPartList();

        final ByteBuf message = adjutant.allocator().buffer(capacity, Integer.MAX_VALUE);
        try {
            message.writeByte(Messages.Q);
            message.writeZero(Messages.LENGTH_SIZE); // placeholder

            for (int i = 0; i < stmtCount; i++) {
                if (i > 0) {
                    message.writeByte(PgConstant.SPACE); // because jdbd-postgre support only the charset that ASCII is one byte
                    message.writeByte(PgConstant.SEMICOLON);
                    message.writeByte(PgConstant.SPACE);
                }
                writeStatement(i, sqlPartList, groupList.get(i), message);
            }

            message.writeByte(Messages.STRING_TERMINATOR);
            Messages.writeLength(message);
            return message;
        } catch (Throwable e) {
            message.release();
            throw PgExceptions.wrapForMessage(e);
        }

    }

    /**
     * @see #writeParamStmt(ParamStmt)
     * @see #writeParamBatchCommand(ParamBatchStmt)
     * @see #writeMultiBindCommand(List)
     */
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
                if (value instanceof String || !(dataType instanceof PgType)) {
                    bindStringToArray(stmtIndex, paramValue, message);
                } else if (value.getClass().isArray()) {
                    bindArrayObject(stmtIndex, paramValue, message);
                } else {
                    throw PgExceptions.nonSupportBindSqlTypeError(stmtIndex, paramValue);
                }
            } else if (dataType instanceof PgType) {
                bindBuildInType(stmtIndex, paramValue, message);
            } else if (isIllegalTypeName(dataType)) {
                throw PgExceptions.errorTypeName(dataType);
            } else if (value instanceof String) {
                message.writeBytes(dataType.typeName().getBytes(clientCharset));
                message.writeByte(PgConstant.SPACE);
                writeBackslashEscapes((String) value, message);
            } else {
                throw PgExceptions.nonSupportBindSqlTypeError(stmtIndex, paramValue);
            }

        }

        message.writeBytes(sqlPartList.get(paramCount).getBytes(clientCharset));

    }

    /**
     * @see <a href="https://www.postgresql.org/docs/current/datatype.html">Data Types</a>
     */
    private void bindBuildInType(final int batchIndex, final ParamValue bindValue, final ByteBuf message)
            throws JdbdException {
        final Charset clientCharset = this.clientCharset;
        final PgType pgType = (PgType) bindValue.getType();
        switch (pgType) {
            case BOOLEAN: {
                final boolean value = PgBinds.bindToBoolean(batchIndex, bindValue);
                message.writeBytes((value ? PgConstant.TRUE : PgConstant.FALSE).getBytes(this.clientCharset));
            }
            break;
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
                final String value;
                value = PgBinds.bindToString(batchIndex, bindValue);

                message.writeBytes(pgType.name().getBytes(clientCharset));
                message.writeByte(PgConstant.SPACE);

                writeBackslashEscapes(value, message);
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

                message.writeBytes("INTERVAL ".getBytes(clientCharset));

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

                final String value;
                value = PgBinds.bindToPostgreDate(batchIndex, bindValue);

                message.writeBytes("DATE ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case TIMESTAMP: {
                final String value;
                value = PgBinds.bindToPostgreTimestamp(batchIndex, bindValue);

                message.writeBytes("TIMESTAMP ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.getBytes(clientCharset));
                message.writeByte(PgConstant.QUOTE);
            }
            break;
            case TIMESTAMPTZ: {
                final String value;
                value = PgBinds.bindToPostgreTimestampTz(batchIndex, bindValue);

                message.writeBytes("TIMESTAMPTZ ".getBytes(clientCharset));

                message.writeByte(PgConstant.QUOTE);
                message.writeBytes(value.getBytes(clientCharset));
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
        if (!(dataType instanceof PgType) && isIllegalTypeName(dataType)) {
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

        // 1. write array value
        final int dimension;
        dimension = writeArrayObject(batchIndex, paramValue, message);

        // 2. write type suffix
        final PgType pgType = ((PgType) paramValue.getType()).elementType();
        assert pgType != null;

        final String typeName = pgType.typeName();
        final StringBuilder builder = new StringBuilder(typeName.length() + 2 + (dimension << 1));
        builder.append("::")
                .append(typeName);

        for (int i = 0; i < dimension; i++) {
            builder.append("[]");
        }

        message.writeBytes(builder.toString().getBytes(this.clientCharset));


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
        message.writeByte(PgConstant.QUOTE);

        if (followChar != PgConstant.NUL) {
            message.setByte(startIndex, 'E');
        }

    }


    private static boolean isIllegalTypeName(final DataType dataType) {
        final String typeName;
        typeName = dataType.typeName();

        if (KEY_WORD_MAP.containsKey(typeName.toUpperCase(Locale.ROOT))) {
            throw PgExceptions.errorTypeName(dataType);
        }
        final int length;
        if (dataType.isArray()) {
            final int index = typeName.lastIndexOf("[]");
            if (index < 1) {
                throw PgExceptions.errorTypeName(dataType);
            }
            length = index;
        } else {
            length = typeName.length();
        }

        boolean match = length > 0;
        char ch;
        for (int i = 0, lastIndex = 0; i < length; i++) {
            ch = typeName.charAt(i);
            if ((ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || ch == '_') {
                continue;
            } else if (i == 0) {
                match = false;
                break;
            } else if ((ch >= '0' && ch <= '9') || ch == '$') {
                continue;
            } else if (ch == PgConstant.SPACE) {
                if (KEY_WORD_MAP.containsKey(typeName.substring(lastIndex, i).toUpperCase(Locale.ROOT))) {
                    match = false;
                    break;
                }
                lastIndex = i + 1;
                continue;
            }
            match = false;
            break;
        }
        return !match;
    }


    /**
     * @see <a href="https://www.postgresql.org/docs/current/sql-keywords-appendix.html#KEYWORDS-TABLE">SQL Key Words</a>
     */
    private static Map<String, Boolean> createKeyWordMap() {
        final Map<String, Boolean> map = PgCollections.hashMap();

        map.put("SELECT", Boolean.TRUE);
        map.put("INSERT", Boolean.TRUE);
        map.put("UPDATE", Boolean.TRUE);
        map.put("DELETE", Boolean.TRUE);

        map.put("FROM", Boolean.TRUE);
        map.put("WHERE", Boolean.TRUE);
        map.put("SET", Boolean.TRUE);
        map.put("AND", Boolean.TRUE);

        map.put("JOIN", Boolean.TRUE);
        map.put("ON", Boolean.TRUE);
        map.put("VALUES", Boolean.TRUE);
        map.put("VALUE", Boolean.TRUE);

        map.put("VIEW", Boolean.TRUE);
        map.put("VIEWS", Boolean.TRUE);
        map.put("WITH", Boolean.TRUE);
        map.put("MERGE", Boolean.TRUE);

        map.put("TABLE", Boolean.TRUE);

        return Collections.unmodifiableMap(map);
    }


}
