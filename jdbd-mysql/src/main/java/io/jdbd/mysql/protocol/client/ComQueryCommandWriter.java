package io.jdbd.mysql.protocol.client;

import io.jdbd.LongDataReadException;
import io.jdbd.mysql.*;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.syntax.MySQLStatement;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.type.geometry.Geometry;
import io.jdbd.vendor.conf.Properties;
import io.jdbd.vendor.util.JdbdBufferUtils;
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
import java.util.*;
import java.util.function.Supplier;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 */
final class ComQueryCommandWriter {


    /**
     * @return a unmodifiable list.
     */
    static List<ByteBuf> createBindableMultiCommand(final List<StmtWrapper> stmtWrapperList
            , Supplier<Integer> sequenceIdSupplier, MySQLTaskAdjutant adjutant)
            throws SQLException, LongDataReadException {
        return new ComQueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeMultiCommand(stmtWrapperList);
    }

    /**
     * @return a unmodifiable list.
     */
    static List<ByteBuf> createBindableCommand(StmtWrapper stmtWrapper, Supplier<Integer> sequenceIdSupplier
            , MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {
        return new ComQueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeBindableCommand(stmtWrapper);
    }

    /**
     * @return a unmodifiable list.
     */
    static List<ByteBuf> createBindableBatchCommand(BatchWrapper wrapper, Supplier<Integer> sequenceIdSupplier
            , MySQLTaskAdjutant adjutant) throws SQLException, LongDataReadException {

        return new ComQueryCommandWriter(sequenceIdSupplier, adjutant)
                .writeBindableBatchCommand(wrapper);
    }

    /**
     * @return a unmodifiable list.
     */
    static List<ByteBuf> createStaticSingleCommand(final String sql, Supplier<Integer> sequenceIdSupplier
            , final MySQLTaskAdjutant adjutant) throws SQLException {

        if (!adjutant.isSingleStmt(sql)) {
            throw MySQLExceptions.createMultiStatementError();
        }

        final byte[] commandBytes = sql.getBytes(adjutant.obtainCharsetClient());
        ByteBuf packet = adjutant.createPacketBuffer(2048);
        packet.writeByte(PacketUtils.COM_QUERY);

        final List<ByteBuf> packetList;
        if (commandBytes.length < PacketUtils.MAX_PAYLOAD) {
            packet.writeBytes(commandBytes);
            PacketUtils.writePacketHeader(packet, sequenceIdSupplier.get());
            packetList = Collections.singletonList(packet);
        } else {
            final LinkedList<ByteBuf> tempPacketList = new LinkedList<>();
            try {
                packet = writeStaticCommand(commandBytes, packet, tempPacketList, sequenceIdSupplier, adjutant);

                PacketUtils.writePacketHeader(packet, sequenceIdSupplier.get());
                tempPacketList.add(packet);

                packetList = MySQLCollections.unmodifiableList(tempPacketList);
            } catch (Throwable e) {
                releaseOnError(tempPacketList, packet);
                throw e;
            }
        }
        return packetList;
    }

    /**
     * @return a unmodifiable list.
     */
    static List<ByteBuf> createStaticMultiCommand(final List<String> sqlList, Supplier<Integer> sequenceIdSupplier
            , final MySQLTaskAdjutant adjutant) throws SQLException {
        if (sqlList.isEmpty()) {
            throw MySQLExceptions.createQueryIsEmptyError();
        }

        final int size = sqlList.size();
        final Charset clientCharset = adjutant.obtainCharsetClient();

        final LinkedList<ByteBuf> packetList = new LinkedList<>();

        ByteBuf packet = adjutant.createPacketBuffer(2048);
        packet.writeByte(PacketUtils.COM_QUERY);

        final byte[] semicolonBytes = Constants.SEMICOLON.getBytes(clientCharset);

        try {
            for (int i = 0; i < size; i++) {
                String sql = sqlList.get(i);
                if (!adjutant.isSingleStmt(sql)) {
                    throw MySQLExceptions.createMultiStatementError();
                }
                if (i > 0) {
                    packet.writeBytes(semicolonBytes);
                }
                byte[] commandBytes = sql.getBytes(clientCharset);
                packet = writeStaticCommand(commandBytes, packet, packetList, sequenceIdSupplier, adjutant);

            }
            PacketUtils.writePacketHeader(packet, sequenceIdSupplier.get());
            packetList.add(packet);
            return MySQLCollections.unmodifiableList(packetList);
        } catch (Throwable e) {
            releaseOnError(packetList, packet);
            throw e;
        }
    }


    private static ByteBuf writeStaticCommand(final byte[] commandBytes, ByteBuf currentPack
            , final List<ByteBuf> packetList, Supplier<Integer> sequenceIdSupplier
            , final MySQLTaskAdjutant adjutant) {

        ByteBuf packet = currentPack;
        for (int offset = 0, length; offset < commandBytes.length; ) {
            if (offset == 0 && packet.readableBytes() == 5) {
                length = Math.min(PacketUtils.MAX_PAYLOAD - 1, commandBytes.length - offset);
            } else {
                length = Math.min(PacketUtils.MAX_PAYLOAD, commandBytes.length - offset);
            }
            packet.writeBytes(commandBytes, offset, length);
            offset += length;

            if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                packet = PacketUtils.addAndCutBigPacket(packet, packetList, sequenceIdSupplier
                        , adjutant.allocator()::buffer);
            }
        }
        return packet;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ComQueryCommandWriter.class);




    private final Supplier<Integer> sequenceIdSupplier;

    private final MySQLTaskAdjutant adjutant;

    private final Properties<PropertyKey> properties;

    private final boolean ansiQuotes;

    private final boolean hexEscape;

    private final Charset clientCharset;

    private final boolean supportStream;

    private ComQueryCommandWriter(Supplier<Integer> sequenceIdSupplier, MySQLTaskAdjutant adjutant) {

        this.sequenceIdSupplier = sequenceIdSupplier;
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();

        Server server = this.adjutant.obtainServer();

        this.ansiQuotes = server.containSqlMode(SQLMode.ANSI_QUOTES);
        this.hexEscape = server.containSqlMode(SQLMode.NO_BACKSLASH_ESCAPES);
        this.clientCharset = adjutant.obtainCharsetClient();
        this.supportStream = this.properties.getOrDefault(PropertyKey.clientPrepare, Enums.ClientPrepare.class)
                != Enums.ClientPrepare.UN_SUPPORT_STREAM;
    }


    /*################################## blow private method ##################################*/


    /**
     * @return a unmodifiable list.
     * @see #createBindableMultiCommand(List, Supplier, MySQLTaskAdjutant)
     */
    private List<ByteBuf> writeMultiCommand(final List<StmtWrapper> stmtWrapperList)
            throws SQLException, LongDataReadException {
        final int size = stmtWrapperList.size();
        final LinkedList<ByteBuf> packetList = new LinkedList<>();
        ByteBuf packet = this.adjutant.createPacketBuffer(2048);
        packet.writeByte(PacketUtils.COM_QUERY);
        try {
            final byte[] semicolonBytes = Constants.SEMICOLON.getBytes(clientCharset);
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    packet.writeBytes(semicolonBytes);
                }
                packet = doWriteBindableCommand(i, stmtWrapperList.get(i), packetList, packet);
                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
                            , this.adjutant.allocator()::buffer);
                }
            }
            PacketUtils.writePacketHeader(packet, this.sequenceIdSupplier.get());
            packetList.add(packet);
            return MySQLCollections.unmodifiableList(packetList);
        } catch (Throwable e) {
            releaseOnError(packetList, packet);
            throw e;
        }
    }

    /**
     * @return a unmodifiable list.
     * @see #createBindableCommand(StmtWrapper, Supplier, MySQLTaskAdjutant)
     */
    private List<ByteBuf> writeBindableCommand(StmtWrapper stmtWrapper) throws SQLException, LongDataReadException {
        LinkedList<ByteBuf> packetList = new LinkedList<>();
        int capacity = stmtWrapper.getSql().length() * this.adjutant.obtainMaxBytesPerCharClient() + 100;
        ByteBuf packet = this.adjutant.createPacketBuffer(capacity);
        try {
            packet.writeByte(PacketUtils.COM_QUERY);
            packet = doWriteBindableCommand(-1, stmtWrapper, packetList, packet);
            PacketUtils.writePacketHeader(packet, this.sequenceIdSupplier.get());
            packetList.add(packet);
            return MySQLCollections.unmodifiableList(packetList);
        } catch (Throwable e) {
            releaseOnError(packetList, packet);
            throw e;
        }
    }

    /**
     * @return a unmodifiable list.
     * @see #createBindableBatchCommand(BatchWrapper, Supplier, MySQLTaskAdjutant)
     */
    private List<ByteBuf> writeBindableBatchCommand(BatchWrapper wrapper) throws SQLException, LongDataReadException {
        final MySQLStatement stmt;
        stmt = this.adjutant.parse(wrapper.getSql());

        final List<List<BindValue>> parameterGroupList = wrapper.getParameterGroupList();
        final int stmtCount = parameterGroupList.size();
        final List<String> staticSqlList = stmt.getStaticSql();


        final boolean supportStream = this.supportStream;
        final Charset clientCharset = this.clientCharset;
        final byte[] nullBytes = Constants.NULL.getBytes(clientCharset);
        final byte[] semicolonBytes = Constants.SEMICOLON.getBytes(clientCharset);

        final LinkedList<ByteBuf> packetList = new LinkedList<>();
        ByteBuf packet = this.adjutant.createPacketBuffer(2048);
        packet.writeByte(PacketUtils.COM_QUERY);

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
                    } else if (bindValue.isStream() && !supportStream) {
                        throw MySQLExceptions.createUnsupportedParamTypeError(i, bindValue);
                    } else if (bindValue.getValue() == null || bindValue.getType() == MySQLType.NULL) {
                        packet.writeBytes(nullBytes);
                        continue;
                    }
                    packet = bindParameter(i, bindValue, packet, packetList); // bind parameter to sql.

                }
                // write last static sql
                packet.writeCharSequence(staticSqlList.get(paramCount), clientCharset);

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
                            , this.adjutant.allocator()::buffer);
                }

            }

            PacketUtils.writePacketHeader(packet, this.sequenceIdSupplier.get());
            packetList.add(packet);
            return MySQLCollections.unmodifiableList(packetList);
        } catch (Throwable e) {
            releaseOnError(packetList, packet);
            throw e;
        }
    }


    /**
     * @see #writeMultiCommand(List)
     * @see #writeBindableCommand(StmtWrapper)
     */
    private ByteBuf doWriteBindableCommand(final int stmtIndex, StmtWrapper stmtWrapper, final List<ByteBuf> packetList
            , final ByteBuf currentPacket) throws SQLException, LongDataReadException {

        MySQLStatement stmt = this.adjutant.parse(stmtWrapper.getSql());
        final List<BindValue> parameterGroup = stmtWrapper.getParameterGroup();
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
            } else if (bindValue.isStream() && !supportStream) {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
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
     * @see #doWriteBindableCommand(int, StmtWrapper, List, ByteBuf)
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
                buffer.writeBytes(BindUtils.bindToBits(stmtIndex, bindValue).getBytes(this.clientCharset));
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
            case GEOMETRY: {
                if (bindValue.getValue() instanceof Geometry) {
                    newBuffer = bindToGeometry(stmtIndex, bindValue, buffer);
                } else {
                    newBuffer = bindToBytes(stmtIndex, bindValue, buffer, packetList);
                }
            }
            break;
            case UNKNOWN:
                //TODO add code
                throw BindUtils.createTypeNotMatchException(bindValue);
            default:
                throw MySQLExceptions.createUnknownEnumException(bindValue.getType());
        }
        return newBuffer;
    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private ByteBuf bindToGeometry(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getRequiredValue();
        if (!(nonNull instanceof Geometry)) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }
        packetBuffer.writeBytes("ST_GeometryFromWKB(".getBytes(this.clientCharset));
        final byte[] wkbBytes = ((Geometry) nonNull).asWkb(false);
        if (this.hexEscape) {
            packetBuffer.writeByte('X');
            packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
            JdbdBufferUtils.writeHexEscapes(packetBuffer, wkbBytes, wkbBytes.length);
        } else {
            packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
            writeByteEscapes(packetBuffer, wkbBytes, wkbBytes.length);
        }
        packetBuffer.writeBytes("')".getBytes(this.clientCharset));
        return packetBuffer;
    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private ByteBuf bindToSetType(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getRequiredValue();
        if (!(nonNull instanceof Set)) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
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
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
            }
            count++;
        }
        final byte[] bytes = builder.toString().getBytes(this.clientCharset);
        if (this.hexEscape) {
            JdbdBufferUtils.writeHexEscapes(packetBuffer, bytes, bytes.length);
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
        final Object nonNull = bindValue.getRequiredValue();

        final String text;
        if (nonNull instanceof String) {
            try {
                text = (String) nonNull;
                BigDecimal number = new BigDecimal(text);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("bind number :{}", number);
                }
            } catch (NumberFormatException e) {
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
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
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }

        buffer.writeBytes(text.getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToBoolean(final int stmtIndex, final BindValue bindValue, final ByteBuf buffer)
            throws SQLException {
        final Object nonNull = bindValue.getRequiredValue();

        final Boolean b;
        if (nonNull instanceof Boolean) {
            b = (Boolean) nonNull;
        } else if (nonNull instanceof String) {
            String text = ((String) nonNull);
            b = MySQLConvertUtils.tryConvertToBoolean(text);
        } else if (nonNull instanceof Number) {
            b = MySQLConvertUtils.tryConvertToBoolean(((Number) nonNull));
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }
        if (b == null) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }
        buffer.writeBytes(b.toString().getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private ByteBuf bindToBytes(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer
            , final List<ByteBuf> packetList) throws SQLException, LongDataReadException {
        final Object nonNull = bindValue.getRequiredValue();

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
                    JdbdBufferUtils.writeHexEscapes(packet, bytes, bytes.length);
                } else {
                    writeByteEscapes(packet, bytes, bytes.length);
                }
            } else if (nonNull instanceof CharSequence) {
                byte[] bytes = nonNull.toString().getBytes(this.clientCharset);
                if (this.hexEscape) {
                    JdbdBufferUtils.writeHexEscapes(packet, bytes, bytes.length);
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
                    JdbdBufferUtils.writeHexEscapes(packet, bytes, bytes.length);
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
                throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
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
        final Object nonNull = bindValue.getRequiredValue();

        final String text;
        if (nonNull instanceof LocalTime) {
            LocalTime time = (LocalTime) nonNull;
            text = OffsetTime.of(time, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime()
                    .format(MySQLTimeUtils.MYSQL_TIME_FORMATTER);
        } else if (nonNull instanceof OffsetTime) {
            text = ((OffsetTime) nonNull).withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime()
                    .format(MySQLTimeUtils.MYSQL_TIME_FORMATTER);
        } else if (nonNull instanceof String) {
            text = parseAndFormatTime(stmtIndex, (String) nonNull, bindValue);
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }

        packetBuffer.writeBytes(text.getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToDate(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getRequiredValue();

        final String text;
        if (nonNull instanceof LocalDate) {
            text = ((LocalDate) nonNull).format(DateTimeFormatter.ISO_LOCAL_DATE);
        } else if (nonNull instanceof String) {
            try {
                text = (String) nonNull;
                LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE);
            } catch (DateTimeParseException e) {
                throw BindUtils.createTypeNotMatchException(bindValue, e);
            }
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }

        packetBuffer.writeBytes(text.getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(int, BindValue, ByteBuf, List)
     */
    private void bindToDateTime(final int stmtIndex, final BindValue bindValue, final ByteBuf packetBuffer)
            throws SQLException {
        final Object nonNull = bindValue.getRequiredValue();

        final String text;
        if (nonNull instanceof LocalDateTime) {
            text = OffsetDateTime.of((LocalDateTime) nonNull, this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime()
                    .format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
        } else if (nonNull instanceof OffsetDateTime) {
            text = ((OffsetDateTime) nonNull).withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime()
                    .format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
        } else if (nonNull instanceof ZonedDateTime) {
            text = ((ZonedDateTime) nonNull).withZoneSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime()
                    .format(MySQLTimeUtils.MYSQL_DATETIME_FORMATTER);
        } else if (nonNull instanceof String) {
            text = parseAndFormatDateTime(stmtIndex, (String) nonNull, bindValue);
        } else {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }
        final String timeParamText = "'" + text + "'";
        packetBuffer.writeBytes(timeParamText.getBytes(this.clientCharset));

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
                    JdbdBufferUtils.writeHexEscapes(packet, bufferArray, byteBuffer.remaining());
                } else {
                    writeByteEscapes(packet, bufferArray, byteBuffer.remaining());
                }
                byteBuffer.clear();
                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
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
                    JdbdBufferUtils.writeHexEscapes(packet, bufferArray, bufferArray.length);
                } else {
                    writeByteEscapes(packet, bufferArray, bufferArray.length);
                }
                //24. clear charBuffer for next
                charBuffer.clear();

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
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
                    JdbdBufferUtils.writeHexEscapes(packet, bufferArray, length);
                } else {
                    writeByteEscapes(packet, bufferArray, length);
                }

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.addAndCutBigPacket(packet, packetList, this.sequenceIdSupplier
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
            time = OffsetTime.of(LocalTime.parse(timeText, MySQLTimeUtils.MYSQL_TIME_FORMATTER)
                    , this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } catch (DateTimeParseException e) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }
        final int index = timeText.lastIndexOf('.');
        return time.format(BindUtils.obtainTimeFormatter(index < 0 ? 0 : timeText.length() - index));

    }


    /**
     * @see #bindToDateTime(int, BindValue, ByteBuf)
     */
    private String parseAndFormatDateTime(final int stmtIndex, final String dateTimeText, final BindValue bindValue)
            throws SQLException {
        final LocalDateTime dateTime;
        try {
            dateTime = OffsetDateTime.of(LocalDateTime.parse(dateTimeText, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER)
                    , this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } catch (DateTimeParseException e) {
            throw MySQLExceptions.createUnsupportedParamTypeError(stmtIndex, bindValue);
        }

        final int index = dateTimeText.lastIndexOf('.');
        return dateTime.format(BindUtils.obtainDateTimeFormatter(index < 0 ? 0 : (dateTimeText.length() - index) - 1));

    }

    /*################################## blow private static method ##################################*/

    /**
     * @see #writeBindableCommand(StmtWrapper)
     * @see #writeMultiCommand(List)
     * @see #writeBindableBatchCommand(BatchWrapper)
     * @see #createStaticSingleCommand(String, Supplier, MySQLTaskAdjutant)
     * @see #createStaticMultiCommand(List, Supplier, MySQLTaskAdjutant)
     */
    private static void releaseOnError(Queue<ByteBuf> queue, ByteBuf packet) {
        ByteBuf byteBuf;
        while ((byteBuf = queue.poll()) != null) {
            byteBuf.release();
        }
        queue.clear();
        if (packet.refCnt() > 0) {
            packet.release();
        }
    }

}
