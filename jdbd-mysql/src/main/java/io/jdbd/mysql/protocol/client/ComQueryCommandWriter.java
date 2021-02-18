package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.SQLBindParameterException;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.Server;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.jdbd.vendor.SQLStatement;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Supplier;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 */
final class ComQueryCommandWriter implements StatementCommandWriter {

    private final static byte[] HEX_DIGITS = new byte[]{
            (byte) '0', (byte) '1', (byte) '2', (byte) '3'
            , (byte) '4', (byte) '5', (byte) '6', (byte) '7'
            , (byte) '8', (byte) '9', (byte) 'A', (byte) 'B'
            , (byte) 'C', (byte) 'D', (byte) 'E', (byte) 'F'};

    private final SQLStatement sqlStatement;

    private final Supplier<Integer> sequenceIdSupplier;

    private final ClientProtocolAdjutant adjutant;

    private final Properties properties;

    private final boolean noAnsiQuotes;

    private final boolean hexEscape;

    private final Charset clientCharset;

    ComQueryCommandWriter(SQLStatement sqlStatement, Supplier<Integer> sequenceIdSupplier
            , ClientProtocolAdjutant adjutant) {
        this.sqlStatement = sqlStatement;
        this.sequenceIdSupplier = sequenceIdSupplier;
        this.adjutant = adjutant;
        this.properties = adjutant.obtainHostInfo().getProperties();

        Server server = this.adjutant.obtainServer();

        this.noAnsiQuotes = server.containSqlMode(SQLMode.ANSI_QUOTES);
        this.hexEscape = server.containSqlMode(SQLMode.NO_BACKSLASH_ESCAPES);
        this.clientCharset = adjutant.obtainCharsetClient();
    }

    @Override
    public Publisher<ByteBuf> writeCommand(final List<BindValue> parameterGroup) {
        final int size = parameterGroup.size();
        if (size != this.sqlStatement.getParamCount()) {
            // hear invoker has bug
            throw new IllegalArgumentException(
                    String.format("parameterGroup BindValue parameter size[%s] and sql parameter size[%s] not match."
                            , size, this.sqlStatement.getParamCount()));
        }

        return Flux.create(sink -> emitComQueryPacket(parameterGroup, sink));
    }


    /*################################## blow private method ##################################*/

    /**
     * @see #writeCommand(List)
     */
    private void emitComQueryPacket(final List<BindValue> parameterGroup, final FluxSink<ByteBuf> sink) {
        final int parameterCount = parameterGroup.size();
        final byte[][] staticSql = this.sqlStatement.getStaticSql();
        if (parameterCount != staticSql.length - 1) {
            SQLBindParameterException e = new SQLBindParameterException(
                    "static sql length[%s] and parameterCount[%s] not match."
                    , staticSql.length, parameterCount);
            sink.error(e);
            return;
        }

        ByteBuf packet = createComQueryPacket();
        int i = 0;
        try {

            final byte[] nullBytes = Constants.NULL.getBytes(this.clientCharset);
            for (i = 0; i < parameterCount; i++) {

                packet.writeBytes(staticSql[i]);
                BindValue bindValue = parameterGroup.get(i);

                if (bindValue.getParamIndex() != i) {
                    // hear invoker has bug
                    BindParameterException e = new BindParameterException(bindValue.getParamIndex()
                            , "BindValue parameter[%s] and position[%s] not match."
                            , bindValue.getParamIndex(), i);
                    sink.error(e);
                    return;
                } else if (bindValue.getValue() == null || bindValue.getType() == MySQLType.NULL) {
                    packet.writeBytes(nullBytes);
                    continue;
                }

                packet = bindParameter(bindValue, packet, sink);

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
            // final write last static sql
            packet.writeBytes(staticSql[i]);

            // this 'if' block handle small packet.
            PacketUtils.publishBigPacket(packet, sink, this.sequenceIdSupplier
                    , this.adjutant::createByteBuffer, true);
        } catch (IOException e) {
            // don't  packet.release(), because bindParameter method have handled.
            sink.error(new BindParameterException(e, i, "Bind parameter[%s] write error.", i));
        } catch (Throwable e) {
            packet.release();
            sink.error(new BindParameterException(e, i, "Bind parameter[%s] write error.", i));
        }

    }

    /**
     * @see #emitComQueryPacket(List, FluxSink)
     */
    private ByteBuf createComQueryPacket() {
        int capacity = 1 + this.sqlStatement.getSql().length();
        ByteBuf packet = this.adjutant.createPacketBuffer(capacity);
        packet.writeByte(PacketUtils.COM_QUERY_HEADER);
        return packet;
    }

    /**
     * @see #emitComQueryPacket(List, FluxSink)
     */
    private ByteBuf bindParameter(final BindValue bindValue, final ByteBuf buffer, final FluxSink<ByteBuf> sink)
            throws Exception {

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
                bindToNumber(bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case BOOLEAN: {
                bindToBoolean(bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case BIT: {
                buffer.writeBytes(BindUtils.bindToBits(bindValue, buffer).getBytes(this.clientCharset));
                newBuffer = buffer;
            }
            break;
            case VARCHAR:
            case CHAR:
            case SET:
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
                newBuffer = bindToBytes(bindValue, buffer, sink);
                break;
            case TIME: {
                bindToTime(bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case DATE: {
                bindToDate(bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case DATETIME:
            case TIMESTAMP: {
                bindToDateTime(bindValue, buffer);
                newBuffer = buffer;
            }
            break;
            case UNKNOWN:
            case GEOMETRY:
                //TODO add code
                throw BindUtils.createTypeNotMatchException(bindValue);
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(bindValue.getType());
        }
        return newBuffer;
    }

    /**
     * @see #bindParameter(BindValue, ByteBuf, FluxSink)
     */
    private void bindToNumber(final BindValue bindValue, final ByteBuf buffer) {
        final Object nonNull = bindValue.getRequiredValue();

        final String text;
        if (nonNull instanceof String) {
            try {
                text = (String) nonNull;
                new BigDecimal(text);
                buffer.writeBytes(text.getBytes(this.clientCharset));
            } catch (NumberFormatException e) {
                throw BindUtils.createTypeNotMatchException(bindValue);
            }
        } else if (nonNull instanceof BigDecimal) {
            text = ((BigDecimal) nonNull).toPlainString();
        } else if (nonNull instanceof Number) {
            text = nonNull.toString();
        } else if (nonNull instanceof Year) {
            text = Integer.toString(((Year) nonNull).getValue());
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        buffer.writeBytes(text.getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(BindValue, ByteBuf, FluxSink)
     */
    private void bindToBoolean(final BindValue bindValue, final ByteBuf buffer) {
        final Object nonNull = bindValue.getRequiredValue();

        final Boolean b;
        if (nonNull instanceof Boolean) {
            b = (Boolean) nonNull;
        } else if (nonNull instanceof String) {
            String text = ((String) nonNull);
            b = MySQLConvertUtils.tryConvertToBoolean(text);
        } else if (nonNull instanceof Number) {
            b = MySQLConvertUtils.tryConvertToBoolean(((BigInteger) nonNull));
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }
        if (b == null) {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }
        buffer.writeBytes(b.toString().getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(BindValue, ByteBuf, FluxSink)
     */
    private ByteBuf bindToBytes(final BindValue bindValue, final ByteBuf packetBuffer, final FluxSink<ByteBuf> sink)
            throws Exception {
        final Object nonNull = bindValue.getRequiredValue();

        ByteBuf newPacket = packetBuffer;
        if (nonNull instanceof byte[]) {
            byte[] bytes = (byte[]) nonNull;
            writeByteEscapes(packetBuffer, bytes, bytes.length);
        } else if (nonNull instanceof CharSequence) {
            byte[] bytes = nonNull.toString().getBytes(this.clientCharset);
            writeByteEscapes(packetBuffer, bytes, bytes.length);
        } else if (nonNull instanceof InputStream) {
            newPacket = writeInputStream(bindValue, packetBuffer, (InputStream) nonNull, sink
                    , this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class));
        } else if (nonNull instanceof Reader) {
            newPacket = writeReader(bindValue, packetBuffer, (Reader) nonNull, sink);
        } else if (nonNull instanceof char[]) {
            byte[] bytes = new String((char[]) nonNull).getBytes(this.clientCharset);
            writeByteEscapes(packetBuffer, bytes, bytes.length);
        } else if (nonNull instanceof ReadableByteChannel) {
            newPacket = writeChannel(bindValue, packetBuffer, (ReadableByteChannel) nonNull, sink);
        } else if (nonNull instanceof Path) {
            try (InputStream input = Files.newInputStream((Path) nonNull, StandardOpenOption.READ)) {
                newPacket = writeInputStream(bindValue, packetBuffer, input, sink, false);
            }
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }
        return newPacket;

    }

    /**
     * @see #bindParameter(BindValue, ByteBuf, FluxSink)
     */
    private void bindToTime(final BindValue bindValue, final ByteBuf packetBuffer) {
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
            text = parseAndFormatTime((String) nonNull, bindValue);
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        packetBuffer.writeBytes(text.getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(BindValue, ByteBuf, FluxSink)
     */
    private void bindToDate(final BindValue bindValue, final ByteBuf packetBuffer) {
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
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        packetBuffer.writeBytes(text.getBytes(this.clientCharset));

    }

    /**
     * @see #bindParameter(BindValue, ByteBuf, FluxSink)
     */
    private void bindToDateTime(final BindValue bindValue, final ByteBuf packetBuffer) {
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
            text = parseAndFormatDateTime((String) nonNull, bindValue);
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }

        packetBuffer.writeBytes(text.getBytes(this.clientCharset));

    }


    /**
     * @see #bindToBytes(BindValue, ByteBuf, FluxSink)
     */
    private ByteBuf writeChannel(final BindValue bindValue, final ByteBuf packetBuffer
            , final ReadableByteChannel channel, final FluxSink<ByteBuf> sink) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final boolean hexEscapes = this.hexEscape;
            // 1. write quote char
            if (hexEscapes) {
                packet.writeByte('X');
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            // 2. write hex or bytes with escapes.
            final byte[] bufferArray = new byte[2048];
            final ByteBuffer byteBuffer = ByteBuffer.wrap(bufferArray);
            while (channel.read(byteBuffer) > 0) {
                if (hexEscapes) {
                    writeHexEscapes(packet, bufferArray, byteBuffer.remaining());
                } else {
                    writeByteEscapes(packet, bufferArray, byteBuffer.remaining());
                }
                byteBuffer.clear();
                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
            // 3. write quote char
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            return packet;
        } catch (IOException e) {
            packet.release();
            throw e;
        } finally {
            if (this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class)) {
                try {
                    channel.close();
                } catch (IOException e) {
                    sink.error(new BindParameterException(
                            e, bindValue.getParamIndex()
                            , "Bind parameter[%s] %s close failure."
                            , channel.getClass().getName()));
                }
            }
        }
    }

    /**
     * @see #bindToBytes(BindValue, ByteBuf, FluxSink)
     */
    private ByteBuf writeReader(final BindValue bindValue, final ByteBuf packetBuffer, final Reader reader
            , final FluxSink<ByteBuf> sink) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final boolean hexEscapes = this.hexEscape;
            // 1. write quote char
            if (hexEscapes) {
                packet.writeByte('X');
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);

            // 2. write hex or bytes with escapes.
            final CharBuffer charBuffer = CharBuffer.allocate(1024);
            final Charset clientCharset = this.clientCharset;
            ByteBuffer byteBuffer;
            byte[] bufferArray;
            while (reader.read(charBuffer) > 0) {//2-1. read char
                byteBuffer = clientCharset.encode(charBuffer); // 2-2. encode char

                //2-3. write bytes with escapes.
                bufferArray = new byte[byteBuffer.remaining()];
                byteBuffer.get(bufferArray);

                if (hexEscapes) {
                    writeHexEscapes(packet, bufferArray, bufferArray.length);
                } else {
                    writeByteEscapes(packet, bufferArray, bufferArray.length);
                }
                //2-4. clear charBuffer for next
                charBuffer.clear();

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
            // 3. write quote char
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            return packet;
        } catch (IOException e) {
            packet.release();
            throw e;
        } finally {
            if (this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class)) {
                try {
                    reader.close();
                } catch (IOException e) {
                    sink.error(new BindParameterException(e, bindValue.getParamIndex()
                            , "Bind parameter[%s] %s close failure."
                            , reader.getClass().getName()));
                }
            }
        }
    }

    /**
     * @see #bindToBytes(BindValue, ByteBuf, FluxSink)
     */
    private ByteBuf writeInputStream(final BindValue bindValue, final ByteBuf packetBuffer, final InputStream input
            , final FluxSink<ByteBuf> sink, final boolean autoClose) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final boolean hexEscapes = this.hexEscape;
            // 1. write quote char
            if (hexEscapes) {
                packet.writeByte('X');
            }
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);

            // 2. write hex or bytes with escapes.
            int length;
            final byte[] bufferArray = new byte[2048];
            while ((length = input.read(bufferArray)) > 0) {
                if (hexEscapes) {
                    writeHexEscapes(packet, bufferArray, length);
                } else {
                    writeByteEscapes(packet, bufferArray, length);
                }

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
            // 3. write quote char
            packet.writeByte(Constants.QUOTE_CHAR_BYTE);
            return packet;
        } catch (IOException e) {
            packet.release();
            throw e;
        } finally {
            if (autoClose) {
                try {
                    input.close();
                } catch (IOException e) {
                    sink.error(new BindParameterException(
                            e, bindValue.getParamIndex()
                            , "Bind parameter[%s] %s close failure."
                            , input.getClass().getName()));
                }
            }
        }
    }


    /**
     * @see #writeInputStream(BindValue, ByteBuf, InputStream, FluxSink, boolean)
     * @see #writeReader(BindValue, ByteBuf, Reader, FluxSink)
     * @see #writeChannel(BindValue, ByteBuf, ReadableByteChannel, FluxSink)
     * @see #bindToBytes(BindValue, ByteBuf, FluxSink)
     */
    private void writeByteEscapes(final ByteBuf buffer, final byte[] bytes, final int length) {
        if (length < 0 || length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                    "length[%s] and bytes.length[%s] not match.", length, bytes.length));
        }
        int lastWritten = 0;
        final boolean noAnsiQuotes = this.noAnsiQuotes;
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];
            if (b == Constants.EMPTY_CHAR_BYTE) {
                if (i > lastWritten) {
                    buffer.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                buffer.writeByte(Constants.BACK_SLASH_BYTE);
                buffer.writeByte('0');
                lastWritten = i + 1;
            } else if (b == Constants.BACK_SLASH_BYTE
                    || b == Constants.QUOTE_CHAR_BYTE
                    || (noAnsiQuotes && b == Constants.DOUBLE_QUOTE_BYTE)) {
                if (i > lastWritten) {
                    buffer.writeBytes(bytes, lastWritten, i - lastWritten);
                }
                buffer.writeByte(Constants.BACK_SLASH_BYTE);
                lastWritten = i; // not i+1 as b wasn't written.
            }

        }

        if (lastWritten < length) {
            buffer.writeBytes(bytes, lastWritten, length - lastWritten);
        }


    }

    /**
     * @see #writeInputStream(BindValue, ByteBuf, InputStream, FluxSink, boolean)
     * @see #writeReader(BindValue, ByteBuf, Reader, FluxSink)
     * @see #writeChannel(BindValue, ByteBuf, ReadableByteChannel, FluxSink)
     * @see #bindToBytes(BindValue, ByteBuf, FluxSink)
     */
    private void writeHexEscapes(final ByteBuf buffer, final byte[] bytes, final int length) {

        final byte[] hexDigits = HEX_DIGITS;
        for (int i = 0; i < length; i++) {
            byte b = bytes[i];

            buffer.writeByte(hexDigits[(b >> 0x4) & 0xF]); // write highBits
            buffer.writeByte(hexDigits[b & 0xF]);          // write lowBits
        }

    }

    /**
     * @see #bindToTime(BindValue, ByteBuf)
     */
    private String parseAndFormatTime(final String timeText, final BindValue bindValue) {
        final LocalTime time;
        try {
            time = OffsetTime.of(LocalTime.parse(timeText, MySQLTimeUtils.MYSQL_TIME_FORMATTER)
                    , this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalTime();
        } catch (DateTimeParseException e) {
            throw BindUtils.createTypeNotMatchException(bindValue, e);
        }
        final int index = timeText.lastIndexOf('.');
        return time.format(BindUtils.obtainTimeFormatter(index < 0 ? 0 : timeText.length() - index));

    }


    /**
     * @see #bindToDateTime(BindValue, ByteBuf)
     */
    private String parseAndFormatDateTime(final String dateTimeText, final BindValue bindValue) {
        final LocalDateTime dateTime;
        try {
            dateTime = OffsetDateTime.of(LocalDateTime.parse(dateTimeText, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER)
                    , this.adjutant.obtainZoneOffsetClient())
                    .withOffsetSameInstant(this.adjutant.obtainZoneOffsetDatabase())
                    .toLocalDateTime();
        } catch (DateTimeParseException e) {
            throw BindUtils.createTypeNotMatchException(bindValue, e);
        }

        final int index = dateTimeText.lastIndexOf('.');
        return dateTime.format(BindUtils.obtainDateTimeFormatter(index < 0 ? 0 : dateTimeText.length() - index));

    }

}
