package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.mysql.util.*;
import io.jdbd.statement.LongDataReadException;
import io.jdbd.vendor.stmt.*;
import io.jdbd.vendor.util.JdbdExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import java.util.function.IntSupplier;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 */
final class QueryCommandWriter {

    /**
     * @param stmt must be below stmt type:
     *             <ul>
     *                  <li>{@link StaticStmt}</li>
     *                  <li>{@link StaticMultiStmt}</li>
     *             </ul>
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    static Publisher<ByteBuf> createStaticCommand(final Stmt stmt, IntSupplier sequenceId,
                                                  final TaskAdjutant adjutant) {
        final Charset clientCharset = adjutant.charsetClient();
        final byte[] sqlBytes;
        if (stmt instanceof StaticStmt) {
            sqlBytes = ((StaticStmt) stmt).getSql().getBytes(clientCharset);
        } else if (stmt instanceof StaticMultiStmt) {
            sqlBytes = ((StaticMultiStmt) stmt).getMultiStmt().getBytes(clientCharset);
        } else {
            throw new IllegalArgumentException("error stmt");
        }
        final ByteBuf packet;
        packet = Packets.createPacket(adjutant.allocator(), stmt);
        packet.writeByte(Packets.COM_QUERY);

        try {
            if (Capabilities.supportQueryAttr(adjutant.capability())) {
                writeQueryAttribute(packet, stmt, adjutant);
            }
            packet.writeBytes(sqlBytes);
            return Packets.createPacketPublisher(packet, sequenceId, adjutant);
        } catch (Throwable e) {
            packet.release();
            throw MySQLExceptions.wrap(e);
        }


    }


    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
     */
    static Publisher<ByteBuf> createStaticBatchCommand(final StaticBatchStmt stmt, IntSupplier sequenceId
            , final TaskAdjutant adjutant) throws SQLException, JdbdSQLException {
        final List<String> sqlGroup = stmt.getSqlGroup();
        if (sqlGroup.isEmpty()) {
            throw MySQLExceptions.createQueryIsEmptyError();
        }
        final int sqlSize = sqlGroup.size();
        final Charset charsetClient = adjutant.charsetClient();
        if (!Charsets.isSupportCharsetClient(charsetClient)) {
            throw MySQLExceptions.notSupportClientCharset(charsetClient);
        }
        final ByteBuf packet;
        packet = Packets.createPacket(adjutant.allocator(), stmt); // 1 representing SEMICOLON_BYTE
        packet.writeByte(Packets.COM_QUERY);

        try {
            if (Capabilities.supportQueryAttr(adjutant.capability())) {
                writeQueryAttribute(packet, stmt, adjutant);
            }
            final MySQLParser sqlParser = adjutant.sqlParser();
            for (int i = 0; i < sqlSize; i++) {
                if (i > 0) {
                    packet.writeByte(Constants.SEMICOLON_BYTE);
                }
                final String sql = sqlGroup.get(i);
                if (!sqlParser.isSingleStmt(sql)) {
                    throw MySQLExceptions.createMultiStatementError();
                }
                packet.writeBytes(sql.getBytes(charsetClient));
            }
            return Packets.createPacketPublisher(packet, sequenceId, adjutant);
        } catch (Throwable e) {
            packet.release();
            throw e;
        }


    }


    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    static Publisher<ByteBuf> createBindableCommand(ParamStmt bindStmt, IntSupplier sequenceId
            , TaskAdjutant adjutant) throws SQLException, LongDataReadException {
        return new QueryCommandWriter(sequenceId, adjutant)
                .writeBindableCommand(bindStmt);
    }


    /**
     * @return a unmodifiable Iterable.
     */
    static Publisher<ByteBuf> createBindableMultiCommand(final ParamMultiStmt stmt, IntSupplier sequenceId
            , TaskAdjutant adjutant)
            throws SQLException, LongDataReadException {
        return new QueryCommandWriter(sequenceId, adjutant)
                .writeMultiCommand(stmt);
    }


    /**
     * @return a unmodifiable list.
     */
    static Publisher<ByteBuf> createBindableBatchCommand(ParamBatchStmt wrapper, IntSupplier sequenceIdSupplier
            , TaskAdjutant adjutant) throws SQLException, LongDataReadException {

        return new QueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeBindableBatchCommand(wrapper);
    }


    private final IntSupplier sequenceId;

    private final TaskAdjutant adjutant;

    private final boolean hexEscape;

    private final Charset clientCharset;


    private QueryCommandWriter(IntSupplier sequenceId, TaskAdjutant adjutant) {
        this.sequenceId = sequenceId;
        this.adjutant = adjutant;
        this.hexEscape = this.adjutant.obtainServer().containSqlMode(SQLMode.NO_BACKSLASH_ESCAPES);
        this.clientCharset = adjutant.charsetClient();
    }


    /*################################## blow private method ##################################*/


    /**
     * @return a unmodifiable list.
     * @see #createBindableMultiCommand(ParamMultiStmt, IntSupplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeMultiCommand(final ParamMultiStmt multiStmt)
            throws SQLException, LongDataReadException {
        final List<ParamStmt> stmtGroup = multiStmt.getStmtList();
        final int size = stmtGroup.size();
        final MySQLParser parser = this.adjutant.sqlParser();
        final ByteBuf packet;
        packet = Packets.createPacket(this.adjutant.allocator(), multiStmt);
        packet.writeByte(Packets.COM_QUERY);
        try {
            if (Capabilities.supportQueryAttr(this.adjutant.capability())) {
                writeQueryAttribute(packet, multiStmt, this.adjutant);
            }
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    packet.writeByte(Constants.SEMICOLON_BYTE);
                }
                final ParamStmt stmt = stmtGroup.get(i);
                final List<String> staticSqlList = parser.parse(stmt.getSql()).getStaticSql();
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
     * @see #createBindableCommand(ParamStmt, IntSupplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeBindableCommand(final ParamStmt stmt) throws SQLException, LongDataReadException {
        final List<String> staticSqlList = this.adjutant.sqlParser().parse(stmt.getSql()).getStaticSql();
        final ByteBuf packet;
        packet = Packets.createPacket(this.adjutant.allocator(), stmt);
        packet.writeByte(Packets.COM_QUERY);
        try {
            if (Capabilities.supportQueryAttr(this.adjutant.capability())) {
                writeQueryAttribute(packet, stmt, this.adjutant);
            }
            doWriteBindableCommand(-1, staticSqlList, stmt.getBindGroup(), packet);
            return Packets.createPacketPublisher(packet, this.sequenceId, this.adjutant);
        } catch (Throwable e) {
            packet.release();
            throw e;
        }

    }

    /**
     * @return a unmodifiable list.
     * @see #createBindableBatchCommand(ParamBatchStmt, IntSupplier, TaskAdjutant)
     */
    private Publisher<ByteBuf> writeBindableBatchCommand(final ParamBatchStmt stmt)
            throws SQLException, LongDataReadException {

        final List<String> staticSqlList = this.adjutant.sqlParser().parse(stmt.getSql()).getStaticSql();

        final List<List<ParamValue>> parameterGroupList = stmt.getGroupList();
        final int stmtCount = parameterGroupList.size();

        final ByteBuf packet;
        packet = Packets.createPacket(this.adjutant.allocator(), stmt);
        packet.writeByte(Packets.COM_QUERY);
        try {
            if (Capabilities.supportQueryAttr(this.adjutant.capability())) {
                writeQueryAttribute(packet, stmt, this.adjutant);
            }
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
     * @see #writeMultiCommand(ParamMultiStmt)
     * @see #writeBindableCommand(ParamStmt)
     */
    private void doWriteBindableCommand(final int batchIndex, final List<String> staticSqlList,
                                        final List<ParamValue> parameterGroup, final ByteBuf packet)
            throws LongDataReadException {

        final int paramCount = staticSqlList.size() - 1;
        MySQLBinds.assertParamCountMatch(batchIndex, paramCount, parameterGroup.size());
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = Constants.NULL.getBytes(clientCharset);

        ParamValue paramValue;
        for (int i = 0; i < paramCount; i++) {
            paramValue = parameterGroup.get(i);
            if (paramValue.getIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.createBindValueParamIndexNotMatchError(batchIndex, paramValue, i);
            }
            packet.writeBytes(staticSqlList.get(i).getBytes(clientCharset));

            final Object value = paramValue.get();
            if (value == null) {
                packet.writeBytes(nullBytes);
                continue;
            }
            if (value instanceof Publisher) {
                throw MySQLExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
            }
            writeParameter(batchIndex, paramValue, packet);
        }
        // write last static sql
        packet.writeBytes(staticSqlList.get(paramCount).getBytes(clientCharset));

    }


    /**
     * @see #doWriteBindableCommand(int, List, List, ByteBuf)
     */
    private void writeParameter(final int batchIndex, final ParamValue paramValue, final ByteBuf packet)
            throws JdbdException {

        try {
            final MySQLType type = (MySQLType) paramValue.getType();
            switch (type) {
                case TINYINT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, Byte.MIN_VALUE, Byte.MAX_VALUE);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case TINYINT_UNSIGNED: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, 0, 0xFF);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case SMALLINT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, Short.MIN_VALUE, Short.MAX_VALUE);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case SMALLINT_UNSIGNED: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, 0, 0xFFFF);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case MEDIUMINT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, MySQLBinds.MEDIUM_INT_MIN_VALUE, MySQLBinds.MEDIUM_INT_MAX_VALUE);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case MEDIUMINT_UNSIGNED: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, 0, 0xFFFF_FF);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case INT: {
                    final int value;
                    value = MySQLBinds.bindToInt(batchIndex, paramValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
                    packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
                }
                break;
                case INT_UNSIGNED: {
                    final long value;
                    value = MySQLBinds.bindToLong(batchIndex, paramValue, 0, 0xFFFF_FFFFL);
                    packet.writeBytes(Long.toString(value).getBytes(this.clientCharset));
                }
                break;
                case BIGINT: {
                    final long value;
                    value = MySQLBinds.bindToLong(batchIndex, paramValue, Long.MIN_VALUE, Long.MAX_VALUE);
                    packet.writeBytes(Long.toString(value).getBytes(this.clientCharset));
                }
                break;
                case BIGINT_UNSIGNED: {
                    final BigInteger value;
                    value = MySQLBinds.bindToBigInteger(batchIndex, paramValue);
                    packet.writeBytes(value.toString().getBytes(this.clientCharset));
                }
                break;
                case DECIMAL:
                case DECIMAL_UNSIGNED: {
                    final BigDecimal value;
                    value = MySQLBinds.bindToDecimal(batchIndex, paramValue);
                    packet.writeBytes(value.toPlainString().getBytes(this.clientCharset));
                }
                break;
                case FLOAT: {
                    final float value;
                    value = MySQLBinds.bindToFloat(batchIndex, paramValue);
                    packet.writeBytes(Float.toString(value).getBytes(this.clientCharset));
                }
                break;
                case DOUBLE: {
                    final double value;
                    value = MySQLBinds.bindToDouble(batchIndex, paramValue);
                    packet.writeBytes(Double.toString(value).getBytes(this.clientCharset));
                }
                break;
                case YEAR:
                    writeYearValue(batchIndex, paramValue, packet);
                    break;
                case BOOLEAN: {
                    final boolean value;
                    value = MySQLBinds.bindToBoolean(batchIndex, paramValue);
                    final String v = value ? Constants.TRUE : Constants.FALSE;
                    packet.writeBytes(v.getBytes(this.clientCharset));
                }
                break;
                case BIT:
                    writeBitValue(batchIndex, paramValue, packet);
                    break;
                case CHAR:
                case VARCHAR:
                case ENUM:
                case TINYTEXT:
                    writeStringValue(batchIndex, paramValue, packet);
                    break;
                case MEDIUMTEXT:
                case TEXT:
                case LONGTEXT:
                case JSON: {
                    if (paramValue.getNonNull() instanceof Path) {
                        writeStringPath(batchIndex, paramValue, packet);
                    } else {
                        writeStringValue(batchIndex, paramValue, packet);
                    }
                }
                break;
                // below binary
                case BINARY:
                case VARBINARY:
                case TINYBLOB:
                    writeBinaryValue(batchIndex, paramValue, packet);
                    break;
                case MEDIUMBLOB:
                case BLOB:
                case LONGBLOB: {
                    if (paramValue.getNonNull() instanceof Path) {
                        writeBinaryPath(batchIndex, paramValue, packet);
                    } else {
                        writeBinaryValue(batchIndex, paramValue, packet);
                    }
                }
                break;
                case SET: {
                    final String value;
                    value = MySQLBinds.bindToSetType(batchIndex, paramValue);
                    writeOneEscapesValue(packet, value.getBytes(this.clientCharset));
                }
                break;
                case TIME:
                    writeTimeValue(batchIndex, paramValue, packet);
                    break;
                case DATE:
                    writeDateValue(batchIndex, paramValue, packet);
                    break;
                case DATETIME:
                case TIMESTAMP:
                    writeDateTimeValue(batchIndex, paramValue, packet);
                    break;
                case GEOMETRY: {
                    final Object nonNull = paramValue.getNonNull();
                    if (nonNull instanceof Path) {
                        writeBinaryPath(batchIndex, paramValue, packet);
                    } else if (nonNull instanceof byte[]) {
                        writeOneEscapesValue(packet, (byte[]) nonNull);
                    } else if (nonNull instanceof String) {
                        writeOneEscapesValue(packet, ((String) nonNull).getBytes(this.clientCharset));
                    } else {
                        throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
                    }
                }
                break;
                case UNKNOWN:
                case NULL:
                default:
                    throw MySQLExceptions.createUnsupportedParamTypeError(batchIndex, paramValue);
            }
        } catch (JdbdException e) {
            throw e;
        } catch (Exception e) {
            throw MySQLExceptions.outOfTypeRange(batchIndex, paramValue, e);
        }

    }


    /**
     * @see #writeStringValue(int, ParamValue, ByteBuf)
     * @see #writeBinaryValue(int, ParamValue, ByteBuf)
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
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeYearValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {
        final Object nonNull = bindValue.getNonNull();
        final int value;
        if (nonNull instanceof Year) {
            value = ((Year) nonNull).getValue();
        } else if (nonNull instanceof Integer) {
            value = (Integer) nonNull;
        } else if (nonNull instanceof Short) {
            value = (Short) nonNull;
        } else {
            throw MySQLExceptions.createNonSupportBindSqlTypeError(batchIndex, bindValue);
        }
        packet.writeBytes(Integer.toString(value).getBytes(this.clientCharset));
    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeBitValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {

        final String value;
        value = bindToBit(batchIndex, bindValue);

        packet.writeByte('B');
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeStringValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {
        final Object nonNull = bindValue.getNonNull();

        final byte[] value;
        if (nonNull instanceof byte[]) {
            if (StandardCharsets.UTF_8.equals(this.clientCharset)) {
                value = (byte[]) nonNull;
            } else {
                value = new String((byte[]) nonNull, StandardCharsets.UTF_8).getBytes(this.clientCharset);
            }
        } else {
            value = MySQLBinds.bindToString(batchIndex, bindValue).getBytes(this.clientCharset);
        }
        writeOneEscapesValue(packet, value);

    }


    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeStringPath(final int batchIndex, final ParamValue bindValue, final ByteBuf packet)
            throws LongDataReadException {

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
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeBinaryValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {
        final Object nonNull = bindValue.getNonNull();
        final byte[] value;
        if (nonNull instanceof byte[]) {
            value = (byte[]) nonNull;
        } else if (nonNull instanceof String) {
            value = ((String) nonNull).getBytes(this.clientCharset);
        } else {
            throw MySQLExceptions.createNonSupportBindSqlTypeError(batchIndex, bindValue);
        }
        writeOneEscapesValue(packet, value);

    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeBinaryPath(final int batchIndex, final ParamValue bindValue, final ByteBuf packet)
            throws LongDataReadException {

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
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeTimeValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {

        final Object nonNull = bindValue.getNonNull();
        final String value;
        if (nonNull instanceof Duration) {
            value = MySQLTimes.durationToTimeText((Duration) nonNull);
        } else {
            value = MySQLBinds.bindToLocalTime(batchIndex, bindValue)
                    .format(MySQLTimes.ISO_LOCAL_TIME_FORMATTER);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);

    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     */
    private void writeDateValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {

        final String value;
        value = MySQLBinds.bindToLocalDate(batchIndex, bindValue)
                .format(DateTimeFormatter.ISO_LOCAL_DATE);

        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
    }

    /**
     * @see #writeParameter(int, ParamValue, ByteBuf)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html">Date and Time Literals</a>
     */
    private void writeDateTimeValue(final int batchIndex, final ParamValue bindValue, final ByteBuf packet) {
        final Object nonNull = bindValue.getNonNull();

        final String value;
        final MySQLServerVersion serverVersion = this.adjutant.handshake10().getServerVersion();

        if ((nonNull instanceof OffsetDateTime || nonNull instanceof ZonedDateTime)
                && serverVersion.meetsMinimum(MySQLServerVersion.V8_0_19)) {
            if (nonNull instanceof OffsetDateTime) {
                value = ((OffsetDateTime) nonNull).format(MySQLTimes.ISO_OFFSET_DATETIME_FORMATTER);
            } else {
                value = ((ZonedDateTime) nonNull).format(MySQLTimes.ISO_OFFSET_DATETIME_FORMATTER);
            }
        } else {
            value = MySQLBinds.bindToLocalDateTime(batchIndex, bindValue)
                    .format(MySQLTimes.ISO_LOCAL_DATETIME_FORMATTER);
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);
        packet.writeBytes(value.getBytes(this.clientCharset));
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);

    }


    /**
     * @see #writeBitValue(int, ParamValue, ByteBuf)
     */
    private static String bindToBit(final int batchIndex, final Value paramValue) {
        final Object nonNull = paramValue.getNonNull();
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
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = MySQLStrings.bitSetToBitString(v, true);
        } else if (nonNull instanceof String) {
            final String v = (String) nonNull;
            if (v.length() > 64 || !MySQLStrings.isBinaryString(v)) {
                throw JdbdExceptions.outOfTypeRange(batchIndex, paramValue);
            }
            value = v;
        } else {
            throw JdbdExceptions.createNonSupportBindSqlTypeError(batchIndex, paramValue);
        }
        return value;
    }


    /**
     * @see #writeBinaryValue(int, ParamValue, ByteBuf)
     * @see #writeStringValue(int, ParamValue, ByteBuf)
     * @see #writeStringPath(int, ParamValue, ByteBuf)
     * @see #writeBinaryPath(int, ParamValue, ByteBuf)
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
                    || b == Constants.DOUBLE_QUOTE_BYTE) {
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


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
     */
    private static void writeQueryAttribute(final ByteBuf packet, final Stmt stmt, final TaskAdjutant adjutant) {

        final List<NamedValue> attrList;
        attrList = stmt.getStmtVarList();

        final int paramCount = attrList.size();
        Packets.writeIntLenEnc(packet, paramCount);// Number of parameters
        Packets.writeIntLenEnc(packet, 1);// Number of parameter sets. Currently, always 1

        if (paramCount == 0) {
            return;
        }

        final byte[] nullBitMap = new byte[(paramCount + 7) >> 3];
        final int nullBitMapWriterIndex = packet.writerIndex();
        packet.writeZero(nullBitMap.length); //placeholder of nullBitMap
        packet.writeByte(1); // new_params_bind_flag.Always 1. Malformed packet error if not 1

        final Charset clientCharset = adjutant.charsetClient();

        NamedValue attr;
        for (int i = 0; i < paramCount; i++) {
            attr = attrList.get(i);
            if (attr.get() == null) {
                nullBitMap[i >> 3] |= (1 << (i & 7));
            } else {
                Packets.writeInt2(packet, ((MySQLType) attr.getType()).parameterType); // param_type_and_flag
                Packets.writeStringLenEnc(packet, attr.getName().getBytes(clientCharset)); // parameter name
            }
        }
        // below write nullBitMap bytes
        final int curWriterIndex = packet.writerIndex();
        packet.writerIndex(nullBitMapWriterIndex);
        packet.writeBytes(nullBitMap);
        packet.writerIndex(curWriterIndex);


        // below write parameter_values for query attribute
        for (int i = 0; i < paramCount; i++) {
            attr = attrList.get(i);
            BinaryWriter.writeBinary(packet, -1, attr.getType(), attr, 6, clientCharset);
        }

    }


}
