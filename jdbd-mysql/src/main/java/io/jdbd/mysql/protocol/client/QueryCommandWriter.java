package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.stmt.BindBatchStmt;
import io.jdbd.mysql.stmt.BindMultiStmt;
import io.jdbd.mysql.stmt.BindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.util.*;
import io.jdbd.stmt.LongDataReadException;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 */
final class QueryCommandWriter {

    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    static Publisher<ByteBuf> createStaticCommand(final String sql, Supplier<Integer> sequenceId
            , final TaskAdjutant adjutant) {
        return Packets.createSimpleCommand(Packets.COM_QUERY, sql, adjutant, sequenceId);
    }


    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
     */
    static Publisher<ByteBuf> createStaticBatchCommand(final StaticBatchStmt stmt, Supplier<Integer> sequenceId
            , final TaskAdjutant adjutant) throws SQLException, JdbdSQLException {
        final List<String> sqlGroup = stmt.getSqlGroup();
        if (sqlGroup.isEmpty()) {
            throw MySQLExceptions.createQueryIsEmptyError();
        }
        final int sqlSize = sqlGroup.size();
        final Charset charsetClient = adjutant.charsetClient();
        final ByteBufAllocator allocator = adjutant.allocator();
        final MySQLParser sqlParser = adjutant.sqlParser();

        final ByteBuf packet = allocator.buffer(100 * sqlSize, Integer.MAX_VALUE); // 1 representing SEMICOLON_BYTE
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header
        packet.writeByte(Packets.COM_QUERY);
        for (int i = 0; i < sqlSize; i++) {
            String sql = sqlGroup.get(i);
            if (!sqlParser.isSingleStmt(sql)) {
                throw MySQLExceptions.createMultiStatementError();
            }
            if (i > 0) {
                packet.writeByte(Constants.SEMICOLON_BYTE);
            }
            packet.writeBytes(sql.getBytes(charsetClient));
        }
        return Packets.createPacketPublisher(packet, sequenceId, adjutant);
    }


    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    static Publisher<ByteBuf> createBindableCommand(BindStmt bindStmt, Supplier<Integer> sequenceId
            , TaskAdjutant adjutant) throws SQLException, LongDataReadException {
        return new QueryCommandWriter(sequenceId, adjutant)
                .writeBindableCommand(bindStmt);
    }


    /**
     * @return a unmodifiable Iterable.
     */
    static Publisher<ByteBuf> createBindableMultiCommand(final BindMultiStmt stmt, Supplier<Integer> sequenceId
            , TaskAdjutant adjutant)
            throws SQLException, LongDataReadException {
        return new QueryCommandWriter(sequenceId, adjutant)
                .writeMultiCommand(stmt.getStmtGroup());
    }


    /**
     * @return a unmodifiable list.
     */
    static Publisher<ByteBuf> createBindableBatchCommand(BindBatchStmt wrapper, Supplier<Integer> sequenceIdSupplier
            , TaskAdjutant adjutant) throws SQLException, LongDataReadException {

        return new QueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeBindableBatchCommand(wrapper);
    }


    private static final Logger LOG = LoggerFactory.getLogger(QueryCommandWriter.class);


    private final Supplier<Integer> sequenceId;

    private final TaskAdjutant adjutant;

    private final boolean hexEscape;

    private final Charset clientCharset;


    private QueryCommandWriter(Supplier<Integer> sequenceId, TaskAdjutant adjutant) {
        this.sequenceId = sequenceId;
        this.adjutant = adjutant;
        this.hexEscape = this.adjutant.obtainServer().containSqlMode(SQLMode.NO_BACKSLASH_ESCAPES);
        this.clientCharset = adjutant.charsetClient();
    }


    /*################################## blow private method ##################################*/


    /**
     * @return a unmodifiable list.
     * @see #createBindableMultiCommand(BindMultiStmt, Supplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeMultiCommand(final List<BindStmt> stmtList)
            throws SQLException, LongDataReadException {

        final int size = stmtList.size();
        final ByteBuf packet = this.adjutant.allocator().buffer(size * 100, Integer.MAX_VALUE);
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header
        packet.writeByte(Packets.COM_QUERY);
        try {
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    packet.writeByte(Constants.SEMICOLON_BYTE);
                }
                final BindStmt stmt = stmtList.get(i);
                final List<String> staticSqlList = this.adjutant.sqlParser().parse(stmt.getSql()).getStaticSql();
                doWriteBindableCommand(i, staticSqlList, stmt.getBindGroup(), packet);
            }
            return Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);
        } catch (Throwable e) {
            packet.release();
            throw e;
        }
    }

    /**
     * @return a unmodifiable list.
     * @see #createBindableCommand(BindStmt, Supplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeBindableCommand(final BindStmt stmt) throws SQLException, LongDataReadException {
        final List<String> staticSqlList = this.adjutant.sqlParser().parse(stmt.getSql()).getStaticSql();
        final ByteBuf packet = this.adjutant.allocator().buffer(1024, Integer.MAX_VALUE);
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header
        packet.writeByte(Packets.COM_QUERY);
        try {
            doWriteBindableCommand(-1, staticSqlList, stmt.getBindGroup(), packet);
            return Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);
        } catch (Throwable e) {
            packet.release();
            throw e;
        }
    }

    /**
     * @return a unmodifiable list.
     * @see #createBindableBatchCommand(BindBatchStmt, Supplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeBindableBatchCommand(BindBatchStmt stmt)
            throws SQLException, LongDataReadException {

        final List<String> staticSqlList = this.adjutant.sqlParser().parse(stmt.getSql()).getStaticSql();

        final List<List<BindValue>> parameterGroupList = stmt.getGroupList();
        final int stmtCount = parameterGroupList.size();

        final ByteBuf packet = this.adjutant.allocator().buffer(stmtCount * 100, Integer.MAX_VALUE);
        packet.writeZero(Packets.HEADER_SIZE); // placeholder of header
        packet.writeByte(Packets.COM_QUERY);
        try {
            for (int i = 0; i < stmtCount; i++) {
                if (i > 0) {
                    packet.writeByte(Constants.SEMICOLON_BYTE); // write ';' delimit multiple statement.
                }
                doWriteBindableCommand(i, staticSqlList, parameterGroupList.get(i), packet);
            }
            return Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);
        } catch (Throwable e) {
            packet.release();
            throw e;
        }
    }


    /**
     * @see #writeMultiCommand(List)
     * @see #writeBindableCommand(BindStmt)
     */
    private void doWriteBindableCommand(final int stmtIndex, final List<String> staticSqlList
            , final List<BindValue> parameterGroup, final ByteBuf packet)
            throws SQLException, LongDataReadException {

        final int paramCount = staticSqlList.size() - 1;
        BindUtils.assertParamCountMatch(stmtIndex, paramCount, parameterGroup.size());
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = Constants.NULL.getBytes(clientCharset);

        for (int i = 0; i < paramCount; i++) {
            BindValue bindValue = parameterGroup.get(i);
            if (bindValue.getIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.createBindValueParamIndexNotMatchError(stmtIndex, bindValue, i);
            }
            packet.writeBytes(staticSqlList.get(i).getBytes(clientCharset));

            final Object value = bindValue.get();
            if (value == null) {
                packet.writeBytes(nullBytes);
                continue;
            }
            if (value instanceof Publisher) {
                throw MySQLExceptions.createNonSupportBindSqlTypeError(stmtIndex, bindValue.getType(), bindValue);
            }
            writeNonNullParameter(stmtIndex, bindValue, packet);
        }
        // write last static sql
        packet.writeBytes(staticSqlList.get(paramCount).getBytes(clientCharset));

    }


    /**
     * @see #doWriteBindableCommand(int, BindStmt, ByteBuf)
     */
    private void writeNonNullParameter(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException, LongDataReadException {

        switch (bindValue.getType()) {
            case TINYINT: {
                final byte value;
                value = MySQLBinds.bindNonNullToByte(batchIndex, bindValue.getType(), bindValue);
                packet.writeBytes(Byte.toString(value).getBytes(this.clientCharset));
            }
            break;
            case TINYINT_UNSIGNED:
            case SMALLINT: {
                final short value;
                value = MySQLBinds.bindNonNullToShort(batchIndex, bindValue.getType(), bindValue);
                packet.writeBytes(Short.toString(value).getBytes(this.clientCharset));
            }
            break;
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT: {
                final int value;
                value = MySQLBinds.bindNonNullToInt(batchIndex, bindValue.getType(), bindValue);
                packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
            }
            break;
            case INT_UNSIGNED:
            case BIGINT: {
                final long value;
                value = MySQLBinds.bindNonNullToLong(batchIndex, bindValue.getType(), bindValue);
                packet.writeBytes(Long.toString(value).getBytes(this.clientCharset));
            }
            break;
            case BIGINT_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED: {
                final BigDecimal value;
                value = MySQLBinds.bindNonNullToDecimal(batchIndex, bindValue.getType(), bindValue);
                packet.writeBytes(value.toPlainString().getBytes(this.clientCharset));
            }
            break;
            case FLOAT:
            case FLOAT_UNSIGNED: {
                final float value;
                value = MySQLBinds.bindNonNullToFloat(batchIndex, bindValue.getType(), bindValue);
                packet.writeBytes(Float.toString(value).getBytes(this.clientCharset));
            }
            break;
            case DOUBLE:
            case DOUBLE_UNSIGNED: {
                final double value;
                value = MySQLBinds.bindNonNullToDouble(batchIndex, bindValue.getType(), bindValue);
                packet.writeBytes(Double.toString(value).getBytes(this.clientCharset));
            }
            break;
            case YEAR: {
                writeYearValue(batchIndex, bindValue, packet);
            }
            break;
            case BOOLEAN: {
                final boolean value;
                value = MySQLBinds.bindNonNullToBoolean(batchIndex, bindValue.getType(), bindValue);
                final String v = value ? Constants.TRUE : Constants.FALSE;
                packet.writeBytes(v.getBytes(this.clientCharset));
            }
            break;
            case BIT: {
                writeBitValue(batchIndex, bindValue, packet);
            }
            break;
            case CHAR:
            case VARCHAR:
            case ENUM:
            case TINYTEXT: {
                writeStringValue(batchIndex, bindValue, packet);
            }
            break;
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
            case JSON: {
                if (bindValue.getNonNull() instanceof Path) {
                    writeStringPath(batchIndex, bindValue, packet);
                } else {
                    writeStringValue(batchIndex, bindValue, packet);
                }
            }
            break;
            // below binary
            case BINARY:
            case VARBINARY:
            case TINYBLOB: {
                writeBinaryValue(batchIndex, bindValue, packet);
            }
            break;
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB: {
                if (bindValue.getNonNull() instanceof Path) {
                    writeBinaryPath(batchIndex, bindValue, packet);
                } else {
                    writeBinaryValue(batchIndex, bindValue, packet);
                }
            }
            break;
            case SET: {
                writeSetTypeValue(batchIndex, bindValue, packet);
            }
            break;
            case TIME: {
                writeTimeValue(batchIndex, bindValue, packet);
            }
            break;
            case DATE: {
                writeDateValue(batchIndex, bindValue, packet);
            }
            break;
            case DATETIME:
            case TIMESTAMP: {
                writeDateTimeValue(batchIndex, bindValue, packet);
            }
            break;
            case GEOMETRY: // GEOMETRY use string or binary
            case UNKNOWN:
            case NULL:
                throw MySQLExceptions.createUnsupportedParamTypeError(batchIndex, bindValue.getType(), bindValue);
            default:
                throw MySQLExceptions.createUnexpectedEnumException(bindValue.getType());
        }

    }


    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeSetTypeValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {
        final Object nonNull = bindValue.getNonNull();
        if (!(nonNull instanceof Set)) {
            throw MySQLExceptions.createUnsupportedParamTypeError(batchIndex, bindValue.getType(), bindValue);
        }
        final Set<?> set = (Set<?>) nonNull;
        final StringBuilder builder = new StringBuilder();
        int count = 0;
        for (Object o : set) {
            if (count > 0) {
                builder.append(",");
            }
            if (o instanceof String) {
                builder.append(o);
            } else if (o instanceof Enum) {
                builder.append(((Enum<?>) o).name());
            } else {
                throw MySQLExceptions.createUnsupportedParamTypeError(batchIndex, bindValue.getType(), bindValue);
            }
            count++;
        }
        writeOneEscapesValue(packet, builder.toString().getBytes(this.clientCharset));

    }

    /**
     * @see #writeStringValue(int, BindValue, ByteBuf)
     * @see #writeBinaryValue(int, BindValue, ByteBuf)
     * @see #writeSetTypeValue(int, BindValue, ByteBuf)
     */
    private void writeOneEscapesValue(final ByteBuf packet, final byte[] value) {
        if (this.hexEscape) {
            packet.writeByte('X');
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            packet.writeBytes(MySQLBuffers.hexEscapes(true, value, value.length));
        } else {
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            writeByteEscapes(packet, value, value.length);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }


    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeYearValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {
        final Object nonNull = bindValue.getNonNull();
        final int value;
        if (nonNull instanceof Year) {
            value = ((Year) nonNull).getValue();
        } else if (nonNull instanceof Integer) {
            value = (Integer) nonNull;
        } else {
            throw MySQLExceptions.createNonSupportBindSqlTypeError(batchIndex, bindValue.getType(), bindValue);
        }
        packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
    }

    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeBitValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {
        final Object nonNull = bindValue.getNonNull();
        final String value;

        if (nonNull instanceof Long) {
            value = Long.toBinaryString((Long) nonNull);
        } else if (nonNull instanceof Integer) {
            value = Integer.toBinaryString((Integer) nonNull);
        } else if (nonNull instanceof Short) {
            value = Integer.toBinaryString(((Short) nonNull) & 0xFFFF);
        } else if (nonNull instanceof Byte) {
            value = Integer.toBinaryString(((Byte) nonNull) & 0xFF);
        } else if (nonNull instanceof BitSet) {
            final BitSet v = (BitSet) nonNull;
            if (v.length() > 64) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
            }
            value = MySQLStrings.bitSetToBitString(v, true);
        } else if (nonNull instanceof String) {
            final String v = (String) nonNull;
            if (!MySQLStrings.isBinaryString(v)) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, bindValue.getType(), bindValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, bindValue.getType(), bindValue);
        }

        packet.writeByte('B');
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }

    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeStringValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {
        final Object nonNull = bindValue.getNonNull();

        final byte[] value;
        if (nonNull instanceof CharSequence) {
            value = nonNull.toString().getBytes(this.clientCharset);
        } else if (nonNull instanceof Enum) {
            value = ((Enum<?>) nonNull).name().getBytes(this.clientCharset);
        } else if (nonNull instanceof byte[]) {
            value = (byte[]) nonNull;
        } else {
            throw MySQLExceptions.createNonSupportBindSqlTypeError(batchIndex, bindValue.getType(), bindValue);
        }

        writeOneEscapesValue(packet, value);

    }


    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeStringPath(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException, LongDataReadException {

        try (InputStream in = Files.newInputStream((Path) bindValue.getNonNull(), StandardOpenOption.READ)) {
            final Charset clientCharset = this.clientCharset;
            final boolean isUtf8 = clientCharset.equals(StandardCharsets.UTF_8);
            final boolean hesEscapes = this.hexEscape;
            if (hesEscapes) {
                packet.writeByte('X');
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            final byte[] buffer = new byte[2048];
            int length;
            while ((length = in.read(buffer)) > 0) {
                if (isUtf8) {
                    if (hesEscapes) {
                        packet.writeBytes(MySQLBuffers.hexEscapes(true, buffer, length));
                    } else {
                        writeByteEscapes(packet, buffer, length);
                    }
                } else {
                    final byte[] bytes;
                    bytes = new String(buffer, 0, length, StandardCharsets.UTF_8)
                            .getBytes(clientCharset);
                    if (hesEscapes) {
                        packet.writeBytes(MySQLBuffers.hexEscapes(true, bytes, bytes.length));
                    } else {
                        writeByteEscapes(packet, bytes, bytes.length);
                    }

                }
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        } catch (Throwable e) {
            if (MySQLExceptions.isByteBufOutflow(e)) {
                throw MySQLExceptions.beyondMessageLength(batchIndex, bindValue);
            } else {
                throw MySQLExceptions.createLongDataReadException(batchIndex, bindValue, e);
            }
        }


    }

    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeBinaryValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {
        final Object nonNull = bindValue.getNonNull();
        if (!(nonNull instanceof byte[])) {
            throw MySQLExceptions.createNonSupportBindSqlTypeError(batchIndex, bindValue.getType(), bindValue);
        }
        writeOneEscapesValue(packet, (byte[]) nonNull);

    }

    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeBinaryPath(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException, LongDataReadException {

        try (InputStream in = Files.newInputStream((Path) bindValue.getNonNull(), StandardOpenOption.READ)) {
            final boolean hexEscapes = this.hexEscape;
            if (hexEscapes) {
                packet.writeByte('X');
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            final byte[] buffer = new byte[2048];
            int length;
            while ((length = in.read(buffer)) > 0) {
                if (hexEscapes) {
                    packet.writeBytes(MySQLBuffers.hexEscapes(true, buffer, length));
                } else {
                    writeByteEscapes(packet, buffer, length);
                }
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        } catch (Throwable e) {
            if (MySQLExceptions.isByteBufOutflow(e)) {
                throw MySQLExceptions.beyondMessageLength(batchIndex, bindValue);
            } else {
                throw MySQLExceptions.createLongDataReadException(batchIndex, bindValue, e);
            }
        }


    }

    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeTimeValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {

        final Object nonNull = bindValue.getNonNull();
        final String value;
        if (nonNull instanceof Duration) {
            value = MySQLTimes.durationToTimeText((Duration) nonNull);
        } else {
            value = MySQLBinds.bindNonNullToLocalTime(batchIndex, bindValue.getType(), bindValue)
                    .format(MySQLTimes.ISO_LOCAL_TIME_FORMATTER);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);

    }

    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     */
    private void writeDateValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {

        final String value;
        value = MySQLBinds.bindNonNullToLocalDate(batchIndex, bindValue.getType(), bindValue)
                .format(DateTimeFormatter.ISO_LOCAL_DATE);

        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }

    /**
     * @see #writeNonNullParameter(int, BindValue, ByteBuf)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html">Date and Time Literals</a>
     */
    private void writeDateTimeValue(final int batchIndex, final BindValue bindValue, final ByteBuf packet)
            throws SQLException {
        final Object nonNull = bindValue.getNonNull();

        final String value;
        final MySQLServerVersion serverVersion = this.adjutant.obtainHandshakeV10Packet().getServerVersion();

        if ((nonNull instanceof OffsetDateTime || nonNull instanceof ZonedDateTime)
                && serverVersion.meetsMinimum(MySQLServerVersion.V8_0_19)) {
            if (nonNull instanceof OffsetDateTime) {
                value = ((OffsetDateTime) nonNull).format(MySQLTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else {
                value = ((ZonedDateTime) nonNull).format(MySQLTimes.ISO_OFFSET_DATETIME_FORMATTER);
            }
        } else {
            value = MySQLBinds.bindNonNullToLocalDateTime(batchIndex, bindValue.getType(), bindValue)
                    .format(MySQLTimes.ISO_LOCAL_DATETIME_FORMATTER);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);

    }


    /**
     * @see #writeBinaryValue(int, BindValue, ByteBuf)
     * @see #writeStringValue(int, BindValue, ByteBuf)
     * @see #writeSetTypeValue(int, BindValue, ByteBuf)
     * @see #writeStringPath(int, BindValue, ByteBuf)
     * @see #writeBinaryPath(int, BindValue, ByteBuf)
     */
    private void writeByteEscapes(final ByteBuf packet, final byte[] bytes, final int length) {
        if (length < 0 || length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                    "length[%s] and bytes.length[%s] not match.", length, bytes.length));
        }
        int lastWritten = 0;
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];
            if (b == Constants.EMPTY_CHAR_BYTE) {
                if (i > lastWritten) {
                    packet.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                packet.writeByte(Constants.BACK_SLASH_BYTE);
                packet.writeByte('0');
                lastWritten = i + 1;
            } else if (b == '\032') {
                if (i > lastWritten) {
                    packet.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                packet.writeByte(Constants.BACK_SLASH_BYTE);
                packet.writeByte('Z');
                lastWritten = i + 1;
            } else if (b == Constants.BACK_SLASH_BYTE
                    || b == Constants.QUOTE_CHAR_BYTE
                    || b == Constants.DOUBLE_QUOTE_BYTE
                    || b == Constants.PERCENT_BYTE
                    || b == Constants.UNDERLINE_BYTE) {
                if (i > lastWritten) {
                    packet.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                packet.writeByte(Constants.BACK_SLASH_BYTE);
                lastWritten = i; // not i+1 as b wasn't written.
            }

        }

        if (lastWritten < length) {
            packet.writeBytes(bytes, lastWritten, length - lastWritten);
        }


    }


    /*################################## blow private static method ##################################*/


}
