package io.jdbd.postgre.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.meta.DataType;
import io.jdbd.postgre.PgConstant;
import io.jdbd.postgre.PgType;
import io.jdbd.postgre.syntax.PgStatement;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.List;

/**
 * <p>
 * This class is writer of postgre simple query protocol.
 * </p>
 * <p>
 * following is chinese signature:<br/>
 * 当你在阅读这段代码时,我才真正在写这段代码,你阅读到哪里,我便写到哪里.
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

            writeStatement(-1, sqlPartList, stmt.getParamGroup(), message);

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

            PgStatement statement;
            ParamStmt stmt;
            final int stmtCount = stmtList.size();
            for (int i = 0; i < stmtCount; i++) {
                stmt = stmtList.get(i);
                statement = adjutant.parse(stmt.getSql());
                if (i > 0) {
                    message.writeByte(PgConstant.SPACE); // because jdbd-postgre support only the charset that ASCII is one byte
                    message.writeByte(PgConstant.SEMICOLON);
                    message.writeByte(PgConstant.SPACE);
                }
                writeStatement(i, statement.sqlPartList(), stmt.getParamGroup(), message);
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





}
