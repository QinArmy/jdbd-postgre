package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.conf.MySQLHost;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.stmt.BatchBindStmt;
import io.jdbd.mysql.stmt.BindValue;
import io.jdbd.mysql.stmt.BindableStmt;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.mysql.util.*;
import io.jdbd.stmt.LongDataReadException;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.stmt.Stmt;
import io.jdbd.vendor.util.JdbdBuffers;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 */
final class ComQueryCommandWriter {


    /**
     * @return a unmodifiable Iterable.
     */
    static Iterable<ByteBuf> createBindableMultiCommand(final List<BindableStmt> bindableStmtList
            , Supplier<Integer> sequenceIdSupplier, TaskAdjutant adjutant)
            throws SQLException, LongDataReadException {
        return new ComQueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeMultiCommand(bindableStmtList);
    }

    /**
     * @return a unmodifiable list.
     */
    static Iterable<ByteBuf> createBindableCommand(BindableStmt bindableStmt, Supplier<Integer> sequenceIdSupplier
            , TaskAdjutant adjutant) throws SQLException, LongDataReadException {
        return new ComQueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeBindableCommand(bindableStmt);
    }

    /**
     * @return a unmodifiable list.
     */
    static Iterable<ByteBuf> createBindableBatchCommand(BatchBindStmt wrapper, Supplier<Integer> sequenceIdSupplier
            , TaskAdjutant adjutant) throws SQLException, LongDataReadException {

        return new ComQueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeBindableBatchCommand(wrapper);
    }

    /**
     * @return a unmodifiable Iterable.
     */
    static Iterable<ByteBuf> createStaticSingleCommand(final Stmt stmt, Supplier<Integer> sequenceIdSupplier
            , final TaskAdjutant adjutant) throws SQLException, JdbdSQLException {
        return Packets.createSimpleCommand(Packets.COM_QUERY, stmt, adjutant, sequenceIdSupplier);
    }

    /**
     * @return a unmodifiable Iterable.
     */
    static Iterable<ByteBuf> createStaticMultiCommand(final List<Stmt> stmtList, Supplier<Integer> sequenceIdSupplier
            , final TaskAdjutant adjutant) throws SQLException, JdbdSQLException {
        if (stmtList.isEmpty()) {
            throw MySQLExceptions.createQueryIsEmptyError();
        }
        final int sqlSize = stmtList.size(), maxAllowedPayload = adjutant.obtainHostInfo().maxAllowedPayload();
        final Charset charsetClient = adjutant.obtainCharsetClient();
        final byte[][] commandArray = new byte[sqlSize][];

        int payloadLength = 1; // COM_QUERY command

        for (int i = 0; i < sqlSize; i++) {
            Stmt stmt = stmtList.get(i);
            String sql = stmt.getSql();
            if (!adjutant.isSingleStmt(sql)) {
                throw MySQLExceptions.createMultiStatementError();
            }
            //TODO zoro append sql timeout hint.
            if (i > 0) {
                payloadLength++; // SEMICOLON_BYTE
            }
            byte[] bytes = sql.getBytes(charsetClient);
            commandArray[i] = bytes;
            payloadLength += bytes.length;
        }
        if (payloadLength < 0 || payloadLength > maxAllowedPayload) {
            throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
        }
        return writeStaticMultiCommand(commandArray, payloadLength, adjutant, sequenceIdSupplier);
    }

    /**
     * @see #createStaticMultiCommand(List, Supplier, TaskAdjutant)
     */
    private static Iterable<ByteBuf> writeStaticMultiCommand(final byte[][] commandArray, final int payloadLength
            , TaskAdjutant adjutant, Supplier<Integer> sequenceIdSupplier) {

        ByteBuf packet;
        if (payloadLength < Packets.MAX_PAYLOAD) {
            packet = adjutant.createPacketBuffer(payloadLength);
        } else {
            packet = adjutant.createPacketBuffer(Packets.MAX_PAYLOAD);
        }
        packet.writeByte(Packets.COM_QUERY);
        List<ByteBuf> packetList = new LinkedList<>();

        for (int i = 0, restPayloadLength = payloadLength - 1; i < commandArray.length; i++) {
            if (i > 0) {
                if (packet.readableBytes() == Packets.MAX_PACKET) {
                    Packets.writePacketHeader(packet, sequenceIdSupplier.get());
                    packetList.add(packet);
                    packet = adjutant.createPacketBuffer(Math.min(Packets.MAX_PAYLOAD, restPayloadLength));
                }
                packet.writeByte(Constants.SEMICOLON_BYTE);
                restPayloadLength--;
            }
            byte[] command = commandArray[i];

            for (int offset = 0, length; offset < command.length; offset += length) {
                if (packet.readableBytes() == Packets.MAX_PACKET) {
                    Packets.writePacketHeader(packet, sequenceIdSupplier.get());
                    packetList.add(packet);
                    packet = adjutant.createPacketBuffer(Math.min(Packets.MAX_PAYLOAD, restPayloadLength));
                }
                length = Math.min(Packets.MAX_PACKET - packet.readableBytes(), command.length - offset);
                packet.writeBytes(command, offset, length);
                restPayloadLength -= length;
            }
        }
        Packets.writePacketHeader(packet, sequenceIdSupplier.get());
        packetList.add(packet);
        if (packet.readableBytes() == Packets.MAX_PACKET) {
            packetList.add(Packets.createEmptyPacket(adjutant.allocator(), sequenceIdSupplier.get()));
        }

        if (packetList.size() == 1) {
            packetList = Collections.singletonList(packetList.get(0));
        } else {
            packetList = Collections.unmodifiableList(packetList);
        }
        return packetList;
    }


    private static final Logger LOG = LoggerFactory.getLogger(ComQueryCommandWriter.class);


    private final Supplier<Integer> sequenceIdSupplier;

    private final TaskAdjutant adjutant;

    private final Properties<PropertyKey> properties;


    private final boolean hexEscape;

    private final Charset clientCharset;

    private final boolean supportStream;

    private ComQueryCommandWriter(Supplier<Integer> sequenceIdSupplier, TaskAdjutant adjutant) {
        this.sequenceIdSupplier = sequenceIdSupplier;
        this.adjutant = adjutant;
        MySQLHost host = adjutant.obtainHostInfo();
        this.properties = host.getProperties();

        Server server = this.adjutant.obtainServer();

        this.hexEscape = server.containSqlMode(SQLMode.NO_BACKSLASH_ESCAPES);
        this.clientCharset = adjutant.obtainCharsetClient();
        this.supportStream = host.clientPrepareSupportStream();
    }


    /*################################## blow private method ##################################*/


    /**
     * @return a unmodifiable list.
     * @see #createBindableMultiCommand(List, Supplier, TaskAdjutant)
     */
    private List<ByteBuf> writeMultiCommand(final List<BindableStmt> bindableStmtList)
            throws SQLException, LongDataReadException {
        final int size = bindableStmtList.size();
        final LinkedList<ByteBuf> packetList = new LinkedList<>();
        ByteBuf packet = this.adjutant.createPacketBuffer(2048);
        packet.writeByte(Packets.COM_QUERY);
        try {
            final byte[] semicolonBytes = Constants.SEMICOLON.getBytes(clientCharset);
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    packet.writeBytes(semicolonBytes);
                }
                packet = doWriteBindableCommand(i, bindableStmtList.get(i), packetList, packet);
                if (packet.readableBytes() >= Packets.MAX_PACKET) {
                    packet = Packets.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
                            , this.adjutant.allocator()::buffer);
                }
            }
            Packets.writePacketHeader(packet, this.sequenceIdSupplier.get());
            packetList.add(packet);
            return MySQLCollections.unmodifiableList(packetList);
        } catch (Throwable e) {
            BindUtils.releaseOnError(packetList, packet);
            throw e;
        }
    }

    /**
     * @return a unmodifiable list.
     * @see #createBindableCommand(BindableStmt, Supplier, TaskAdjutant)
     */
    private Iterable<ByteBuf> writeBindableCommand(BindableStmt bindableStmt) throws SQLException, LongDataReadException {
        LinkedList<ByteBuf> packetList = new LinkedList<>();
        int capacity = bindableStmt.getSql().length() * this.adjutant.obtainMaxBytesPerCharClient() + 100;
        ByteBuf packet = this.adjutant.createPacketBuffer(capacity);
        try {
            packet.writeByte(Packets.COM_QUERY);
            packet = doWriteBindableCommand(-1, bindableStmt, packetList, packet);
            Packets.writePacketHeader(packet, this.sequenceIdSupplier.get());
            packetList.add(packet);
            return MySQLCollections.unmodifiableList(packetList);
        } catch (Throwable e) {
            BindUtils.releaseOnError(packetList, packet);
            throw e;
        }
    }

    /**
     * @return a unmodifiable list.
     * @see #createBindableBatchCommand(BatchBindStmt, Supplier, TaskAdjutant)
     */
    private Iterable<ByteBuf> writeBindableBatchCommand(BatchBindStmt wrapper) throws SQLException, LongDataReadException {
        final MySQLStatement stmt;
        stmt = this.adjutant.parse(wrapper.getSql());

        final List<List<BindValue>> parameterGroupList = wrapper.getGroupList();
        final int stmtCount = parameterGroupList.size();
        final List<String> staticSqlList = stmt.getStaticSql();


        final boolean supportStream = this.supportStream;
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = Constants.NULL.getBytes(clientCharset);
        final byte[] semicolonBytes = Constants.SEMICOLON.getBytes(clientCharset);

        final LinkedList<ByteBuf> packetList = new LinkedList<>();
        ByteBuf packet = this.adjutant.createPacketBuffer(2048);
        packet.writeByte(Packets.COM_QUERY);

        try {
            for (int i = 0; i < stmtCount; i++) {
                List<BindValue> parameterGroup = parameterGroupList.get(i);
                BindUtils.assertParamCountMatch(i, stmt.getParamCount(), parameterGroup.size());

                if (i > 0) {
                    packet.writeBytes(semicolonBytes); // write ';' delimit multiple statement.
                }
                final int paramCount = parameterGroup.size();
                for (int j = 0; j < paramCount; j++) {
                    packet.writeCharSequence(staticSqlList.get(j), clientCharset);  // write static sql

                    BindValue bindValue = parameterGroup.get(j);

                    if (bindValue.getParamIndex() != j) {
                        // hear invoker has bug
                        throw MySQLExceptions.createBindValueParamIndexNotMatchError(i, bindValue, j);
                    } else if (bindValue.isLongData() && !supportStream) {
                        throw MySQLExceptions.createUnsupportedParamTypeError(i, bindValue.getType(), bindValue);
                    } else if (bindValue.getValue() == null || bindValue.getType() == MySQLType.NULL) {
                        packet.writeBytes(nullBytes);
                        continue;
                    }
                    packet = bindParameter(i, bindValue, packet, packetList); // bind parameter to sql.

                }
                // write last static sql
                packet.writeCharSequence(staticSqlList.get(paramCount), clientCharset);

                if (packet.readableBytes() >= Packets.MAX_PACKET) {
                    packet = Packets.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
                            , this.adjutant.allocator()::buffer);
                }

            }

            Packets.writePacketHeader(packet, this.sequenceIdSupplier.get());
            packetList.add(packet);
            return MySQLCollections.unmodifiableList(packetList);
        } catch (Throwable e) {
            BindUtils.releaseOnError(packetList, packet);
            throw e;
        }
    }


    /**
     * @see #writeMultiCommand(List)
     * @see #writeBindableCommand(BindableStmt)
     */
    private ByteBuf doWriteBindableCommand(final int stmtIndex, BindableStmt bindableStmt, final List<ByteBuf> packetList
            , final ByteBuf currentPacket) throws SQLException, LongDataReadException {

        MySQLStatement stmt = this.adjutant.parse(bindableStmt.getSql());
        final List<BindValue> parameterGroup = bindableStmt.getParamGroup();
        final int paramCount = parameterGroup.size();

        BindUtils.assertParamCountMatch(stmtIndex, stmt.getParamCount(), paramCount);

        ByteBuf packet = currentPacket;

        final List<String> staticSqlList = stmt.getStaticSql();
        final boolean supportStream = this.supportStream;
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = Constants.NULL.getBytes(clientCharset);

        for (int i = 0; i < paramCount; i++) {
            packet.writeBytes(staticSqlList.get(i).getBytes(clientCharset));
            BindValue bindValue = parameterGroup.get(i);
            if (bindValue.getParamIndex() != i) {
                // hear invoker has bug
                throw MySQLExceptions.createBindValueParamIndexNotMatchError(stmtIndex, bindValue, i);
            } else if (bindValue.isLongData() && !supportStream) {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
            } else if (bindValue.getValue() == null || bindValue.getType() == MySQLType.NULL) {
                packet.writeBytes(nullBytes);
                continue;
            }
            packet = bindParameter(stmtIndex, bindValue, packet, packetList);
        }
        // write last static sql
        packet.writeCharSequence(staticSqlList.get(paramCount), clientCharset);
        return packet;
    }


    /**
     * @see #doWriteBindableCommand(int, BindableStmt, List, ByteBuf)
     */
    private ByteBuf bindParameter(final int stmtIndex, final BindValue bindValue, final ByteBuf buffer
            , final List<ByteBuf> packetList) throws SQLException, LongDataReadException {

        final ByteBuf newBuffer;

        switch (bindValue.getType()) {
            case TINYINT:
            case TINYINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case INT:
            case INT_UNSIGNED:
            case BIGINT:
            case BIGINT_UNSIGNED:
            case FLOAT:
            case FLOAT_UNSIGNED:
            case DOUBLE:
            case DOUBLE_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case YEAR: {
                bindToNumber(stmtIndex, bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case BOOLEAN: {
                bindToBoolean(stmtIndex, bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case BIT: {
                long bits = BindUtils.bindToBits(stmtIndex, bindValue.getType(), bindValue, this.clientCharset);
                String bitLiteral = ("B'" + Long.toBinaryString(bits) + "'");
                buffer.writeBytes(bitLiteral.getBytes(this.clientCharset));
                newBuffer = buffer;
            }
            break;
            case VARCHAR:
            case CHAR:
            case JSON:
            case ENUM:
            case TINYTEXT:
            case MEDIUMTEXT:
            case TEXT:
            case LONGTEXT:
                // below binary
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:
            case GEOMETRY:
                newBuffer = bindToBytes(stmtIndex, bindValue, buffer, packetList);
                break;
            case SET: {
                if (bindValue.getValue() instanceof Set) {
                    bindToSetType(stmtIndex, bindValue, buffer);
                    newBuffer = buffer;
                } else {
                    newBuffer = bindToBytes(stmtIndex, bindValue, buffer, packetList);
                }
            }
            break;
            case TIME: {
                bindToTime(stmtIndex, bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case DATE: {
                bindToDate(stmtIndex, bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case DATETIME:
            case TIMESTAMP: {
                bindToDateTime(stmtIndex, bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case UNKNOWN:
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
            default:
                throw MySQLExceptions.createUnknownEnumException(bindValue.getType());
        }
        return newBuffer;
    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    @Deprecated
    private ByteBuf bindToGeometry(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer
            , final List<ByteBuf> packetList) throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        ByteBuf packet = packetBuffer;
        if (nonNull instanceof byte[]
                || nonNull instanceof InputStream
                || nonNull instanceof ReadableByteChannel
                || nonNull instanceof Path) {
            packet.writeBytes("ST_GeometryFromWKB(".getBytes(this.clientCharset));
        } else if (nonNull instanceof String
                || nonNull instanceof Reader) {
            packet.writeBytes("ST_GeomFromText(".getBytes(this.clientCharset));
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        if (this.hexEscape) {
            packet.writeByte('X');
        }
        packet.writeByte(Constants.QUOTE_CHAR_BYTE);

        try {
            if (nonNull instanceof byte[]) {
                byte[] bytes = (byte[]) nonNull;
                if (this.hexEscape) {
                    MySQLBuffers.writeUpperCaseHexEscapes(packet, bytes, bytes.length);
                } else {
                    writeByteEscapes(packet, bytes, bytes.length);
                }
            } else if (nonNull instanceof InputStream) {
                boolean autoClose = this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class);
                packet = writeInputStream(packet, (InputStream) nonNull, packetList, autoClose);
            } else if (nonNull instanceof ReadableByteChannel) {
                packet = writeChannel(packet, (ReadableByteChannel) nonNull, packetList);
            } else if (nonNull instanceof Path) {
                try (InputStream in = Files.newInputStream((Path) nonNull, StandardOpenOption.READ)) {
                    packet = writeInputStream(packet, in, packetList, true);
                }
            } else if (nonNull instanceof String) {
                byte[] bytes = ((String) nonNull).getBytes(this.clientCharset);
                if (this.hexEscape) {
                    MySQLBuffers.writeUpperCaseHexEscapes(packet, bytes, bytes.length);
                } else {
                    writeByteEscapes(packet, bytes, bytes.length);
                }
            } else {
                // Reader
                packet = writeReader(packet, (Reader) nonNull, packetList);
            }
        } catch (IOException e) {
            throw MySQLExceptions.createLongDataReadException(stmtIndex, bindValue, e);
        }
        packet.writeBytes("')".getBytes(this.clientCharset));
        return packet;
    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private ByteBuf bindToSetType(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();
        if (!(nonNull instanceof Set)) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        Set<?> set = (Set<?>) nonNull;
        if (this.hexEscape) {
            packetBuffer.writeByte('X');
        }
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE); //1. write start quote
        StringBuilder builder = new StringBuilder();
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
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
            }
            count++;
        }
        final byte[] bytes = builder.toString().getBytes(this.clientCharset);
        if (this.hexEscape) {
            JdbdBuffers.writeUpperCaseHexEscapes(packetBuffer, bytes, bytes.length);
        } else {
            writeByteEscapes(packetBuffer, bytes, bytes.length);
        }
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);//3. write end quote
        return packetBuffer;
    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToNumber(final int stmtIndex, final BindValue bindValue, final ByteBuf buffer)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();

        final String text;
        if (nonNull instanceof String) {
            try {
                text = (String) nonNull;
                BigDecimal number = new BigDecimal(text);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("bind number :{}", number);
                }
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
            }
        } else if (nonNull instanceof BigDecimal) {
            text = ((BigDecimal) nonNull).toPlainString();
        } else if (nonNull instanceof Number) {
            text = nonNull.toString();
        } else if (nonNull instanceof Year) {
            text = Integer.toString(((Year) nonNull).getValue());
        } else if (nonNull instanceof Boolean) {
            text = (Boolean) nonNull ? "1" : "0";
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }

        buffer.writeBytes(text.getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToBoolean(final int stmtIndex, final BindValue bindValue, final ByteBuf buffer)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();

        final Boolean b;
        if (nonNull instanceof Boolean) {
            b = (Boolean) nonNull;
        } else if (nonNull instanceof String) {
            String text = ((String) nonNull);
            b = MySQLConvertUtils.tryConvertToBoolean(text);
        } else if (nonNull instanceof Number) {
            b = MySQLConvertUtils.tryConvertToBoolean(((Number) nonNull));
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        if (b == null) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        buffer.writeBytes(b.toString().getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private ByteBuf bindToBytes(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer
            , final List<ByteBuf> packetList) throws SQLException, LongDataReadException {
        final Object nonNull = bindValue.getNonNullValue();

        ByteBuf packet = packetBuffer;
        try {
            if (this.hexEscape) {
                packet.writeByte('X');
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE); //1. write start quote
            // 2. below if-else block write bytes.
            if (nonNull instanceof byte[]) {
                byte[] bytes = (byte[]) nonNull;
                if (this.hexEscape) {
                    JdbdBuffers.writeUpperCaseHexEscapes(packet, bytes, bytes.length);
                } else {
                    writeByteEscapes(packet, bytes, bytes.length);
                }
            } else if (nonNull instanceof CharSequence) {
                byte[] bytes = nonNull.toString().getBytes(this.clientCharset);
                if (this.hexEscape) {
                    JdbdBuffers.writeUpperCaseHexEscapes(packet, bytes, bytes.length);
                } else {
                    writeByteEscapes(packet, bytes, bytes.length);
                }
            } else if (nonNull instanceof Enum) {
                packet.writeBytes(((Enum<?>) nonNull).name().getBytes(this.clientCharset));
            } else if (nonNull instanceof InputStream) {
                packet = writeInputStream(packet, (InputStream) nonNull, packetList
                        , this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class));
            } else if (nonNull instanceof Reader) {
                packet = writeReader(packet, (Reader) nonNull, packetList);
            } else if (nonNull instanceof char[]) {
                byte[] bytes = new String((char[]) nonNull).getBytes(this.clientCharset);
                if (this.hexEscape) {
                    JdbdBuffers.writeUpperCaseHexEscapes(packet, bytes, bytes.length);
                } else {
                    writeByteEscapes(packet, bytes, bytes.length);
                }
            } else if (nonNull instanceof ReadableByteChannel) {
                packet = writeChannel(packet, (ReadableByteChannel) nonNull, packetList);
            } else if (nonNull instanceof Path) {
                try (InputStream input = Files.newInputStream((Path) nonNull, StandardOpenOption.READ)) {
                    packet = writeInputStream(packet, input, packetList, false);
                }
            } else {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);//3. write end quote
            return packet;
        } catch (IOException e) {
            throw MySQLExceptions.createLongDataReadException(stmtIndex, bindValue, e);
        }

    }


    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToTime(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();

        final String text;
        if (nonNull instanceof LocalTime) {
            LocalTime time = (LocalTime) nonNull;
            text = OffsetTime.of(time, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime()
                    .format(MySQLTimes.MYSQL_TIME_FORMATTER);
        } else if (nonNull instanceof OffsetTime) {
            text = ((OffsetTime) nonNull).withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime()
                    .format(MySQLTimes.MYSQL_TIME_FORMATTER);
        } else if (nonNull instanceof String) {
            text = parseAndFormatTime(stmtIndex, (String) nonNull, bindValue);
        } else if (nonNull instanceof Duration) {
            try {
                text = MySQLTimes.durationToTimeText((Duration) nonNull);
            } catch (Throwable e) {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
            }
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
        packetBuffer.writeBytes(text.getBytes(this.clientCharset));
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);

    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToDate(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();

        final String text;
        if (nonNull instanceof LocalDate) {
            text = ((LocalDate) nonNull).format(DateTimeFormatter.ISO_LOCAL_DATE);
        } else if (nonNull instanceof String) {
            try {
                text = (String) nonNull;
                LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE);
            } catch (DateTimeParseException e) {
                throw MySQLExceptions.createTypeNotMatchException(stmtIndex, bindValue.getType(), bindValue, e);
            }
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
        packetBuffer.writeBytes(text.getBytes(this.clientCharset));
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToDateTime(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getNonNullValue();

        final String text;
        if (nonNull instanceof LocalDateTime) {
            text = OffsetDateTime.of((LocalDateTime) nonNull, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime()
                    .format(MySQLTimes.MYSQL_DATETIME_FORMATTER);
        } else if (nonNull instanceof OffsetDateTime) {
            text = ((OffsetDateTime) nonNull).withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime()
                    .format(MySQLTimes.MYSQL_DATETIME_FORMATTER);
        } else if (nonNull instanceof ZonedDateTime) {
            text = ((ZonedDateTime) nonNull).withZoneSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime()
                    .format(MySQLTimes.MYSQL_DATETIME_FORMATTER);
        } else if (nonNull instanceof String) {
            text = parseAndFormatDateTime(stmtIndex, (String) nonNull, bindValue);
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
        packetBuffer.writeBytes(text.getBytes(this.clientCharset));
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);

    }


    /**
     * @see #bindToBytes(int, BindValue, ByteBuf, List)
     */
    private ByteBuf writeChannel(final ByteBuf packetBuffer
            , final ReadableByteChannel channel, final List<ByteBuf> packetList) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final boolean hexEscapes = this.hexEscape;
            final byte[] bufferArray = new byte[2048];
            final ByteBuffer byteBuffer = ByteBuffer.wrap(bufferArray);
            while (channel.read(byteBuffer) > 0) {
                if (hexEscapes) {
                    JdbdBuffers.writeUpperCaseHexEscapes(packet, bufferArray, byteBuffer.remaining());
                } else {
                    writeByteEscapes(packet, bufferArray, byteBuffer.remaining());
                }
                byteBuffer.clear();
                if (packet.readableBytes() >= Packets.MAX_PACKET) {
                    packet = Packets.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
                            , this.adjutant.allocator()::buffer);
                }
            }
            return packet;
        } catch (IOException e) {
            if (packet != packetBuffer) {
                packet.release();
                packet = null;
            }
            throw e;
        } finally {
            if (this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class)) {
                try {
                    channel.close();
                } catch (IOException ex) {
                    if (packet != null && packet != packetBuffer) {
                        packet.release();
                    }
                    throw ex;
                }
            }
        }
    }

    /**
     * @see #bindToBytes(int, BindValue, ByteBuf, List)
     */
    private ByteBuf writeReader(final ByteBuf packetBuffer, final Reader reader
            , final List<ByteBuf> packetList) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final boolean hexEscapes = this.hexEscape;

            final CharBuffer charBuffer = CharBuffer.allocate(1024);
            final Charset clientCharset = this.clientCharset;
            ByteBuffer byteBuffer;
            byte[] bufferArray;
            while (reader.read(charBuffer) > 0) {//1. read char
                byteBuffer = clientCharset.encode(charBuffer); // 2. encode char

                //3. write bytes with escapes.
                bufferArray = new byte[byteBuffer.remaining()];
                byteBuffer.get(bufferArray);

                if (hexEscapes) {
                    JdbdBuffers.writeUpperCaseHexEscapes(packet, bufferArray, bufferArray.length);
                } else {
                    writeByteEscapes(packet, bufferArray, bufferArray.length);
                }
                //24. clear charBuffer for next
                charBuffer.clear();

                if (packet.readableBytes() >= Packets.MAX_PACKET) {
                    packet = Packets.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
                            , this.adjutant.allocator()::buffer);
                }
            }
            return packet;
        } catch (IOException e) {
            if (packet != packetBuffer) {
                packet.release();
                packet = null;
            }
            throw e;
        } finally {
            if (this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class)) {
                try {
                    reader.close();
                } catch (IOException ex) {
                    if (packet != null && packet != packetBuffer) {
                        packet.release();
                    }
                    throw ex;
                }
            }
        }
    }

    /**
     * @see #bindToBytes(int, BindValue, ByteBuf, List)
     * @see #bindToGeometry(int, BindValue, ByteBuf, List)
     */
    private ByteBuf writeInputStream(final ByteBuf packetBuffer, final InputStream input
            , final List<ByteBuf> packetList, final boolean autoClose) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final boolean hexEscape = this.hexEscape;
            int length;
            final byte[] bufferArray = new byte[2048];
            while ((length = input.read(bufferArray)) > 0) {
                if (hexEscape) {
                    JdbdBuffers.writeUpperCaseHexEscapes(packet, bufferArray, length);
                } else {
                    writeByteEscapes(packet, bufferArray, length);
                }

                if (packet.readableBytes() >= Packets.MAX_PACKET) {
                    packet = Packets.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
                            , this.adjutant.allocator()::buffer);
                }
            }
            return packet;
        } catch (IOException e) {
            if (packet != packetBuffer) {
                packet.release();
                packet = null;
            }
            throw e;
        } finally {
            if (autoClose) {
                try {
                    input.close();
                } catch (IOException ex) {
                    if (packet != null && packet != packetBuffer) {
                        packet.release();
                    }
                    throw ex;
                }
            }
        }

    }


    /**
     * @see #writeInputStream(ByteBuf, InputStream, List, boolean)
     * @see #writeReader(ByteBuf, Reader, List)
     * @see #writeChannel(ByteBuf, ReadableByteChannel, List)
     * @see #bindToBytes(int, BindValue, ByteBuf, List)
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

    /**
     * @see #bindToTime(int, BindValue, ByteBuf)
     */
    private String parseAndFormatTime(final int stmtIndex, final String timeText, final BindValue bindValue)
            throws SQLException {
        final LocalTime time;
        try {
            time = OffsetTime.of(LocalTime.parse(timeText, MySQLTimes.MYSQL_TIME_FORMATTER)
                    , this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } catch (DateTimeParseException e) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }
        return time.format(MySQLTimes.obtainTimeFormatterByText(timeText));

    }


    /**
     * @see #bindToDateTime(int, BindValue, ByteBuf)
     */
    private String parseAndFormatDateTime(final int stmtIndex, final String dateTimeText, final BindValue bindValue)
            throws SQLException {
        final LocalDateTime dateTime;
        try {
            dateTime = OffsetDateTime.of(LocalDateTime.parse(dateTimeText, MySQLTimes.MYSQL_DATETIME_FORMATTER)
                    , this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } catch (DateTimeParseException e) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue.getType(), bindValue);
        }

        return dateTime.format(MySQLTimes.obtainDateTimeFormatterByText(dateTimeText));

    }

    /*################################## blow private static method ##################################*/


}
