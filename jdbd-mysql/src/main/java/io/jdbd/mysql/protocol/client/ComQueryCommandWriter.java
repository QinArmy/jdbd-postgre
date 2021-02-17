package io.jdbd.mysql.protocol.client;

import io.jdbd.BindParameterException;
import io.jdbd.SQLBindParameterException;
import io.jdbd.mysql.SQLMode;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLConvertUtils;
import io.jdbd.mysql.util.MySQLExceptionUtils;
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
import java.time.Year;
import java.util.List;
import java.util.function.Supplier;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html">Protocol::COM_QUERY</a>
 */
final class ComQueryCommandWriter implements StatementCommandWriter {

    private final SQLStatement sqlStatement;

    private final Supplier<Integer> sequenceIdSupplier;

    private final ClientProtocolAdjutant adjutant;

    private final Properties properties;

    private final boolean noAnsiQuotes;

    private final Charset clientCharset;

    ComQueryCommandWriter(SQLStatement sqlStatement, Supplier<Integer> sequenceIdSupplier
            , ClientProtocolAdjutant adjutant) {
        this.sqlStatement = sqlStatement;
        this.sequenceIdSupplier = sequenceIdSupplier;
        this.adjutant = adjutant;

        this.properties = adjutant.obtainHostInfo().getProperties();
        this.noAnsiQuotes = !this.adjutant.obtainServer().containSqlMode(SQLMode.ANSI_QUOTES);
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
            final Charset charset = this.adjutant.obtainCharsetClient();
            final byte[] nullBytes = Constants.NULL.getBytes(charset);
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
            // don't  packet.release();
            sink.error(new BindParameterException(e, i, "Bind parameter[%s] write error.", i));
        } catch (Throwable e) {
            packet.release();
            sink.error(new BindParameterException(e, i, "Bind parameter[%s] write error.", i));
        }

    }

    private ByteBuf createComQueryPacket() {
        int capacity = 1 + this.sqlStatement.getSql().length();
        ByteBuf packet = this.adjutant.createPacketBuffer(capacity);
        packet.writeByte(PacketUtils.COM_QUERY_HEADER);
        return packet;
    }


    private ByteBuf bindParameter(final BindValue bindValue, final ByteBuf buffer, final FluxSink<ByteBuf> sink)
            throws Exception {
        ByteBuf newBuffer = buffer;

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
            case YEAR:
                bindToNumber(bindValue, buffer);
                break;
            case BOOLEAN:
                bindToBoolean(bindValue, buffer);
                break;
            case BIT:
                buffer.writeBytes(BindUtils.bindToBits(bindValue, buffer).getBytes(this.clientCharset));
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
            case TIME:
                break;
            case DATE:
            case DATETIME:
            case TIMESTAMP:

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
        // 1. write quote char
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
        // 2. write bytes with escapes.
        if (nonNull instanceof byte[]) {
            byte[] bytes = (byte[]) nonNull;
            writeByteEscapes(packetBuffer, bytes, bytes.length);
        } else if (nonNull instanceof CharSequence) {
            byte[] bytes = nonNull.toString().getBytes(this.clientCharset);
            writeByteEscapes(packetBuffer, bytes, bytes.length);
        } else if (nonNull instanceof InputStream) {
            newPacket = writeInputStream(bindValue, packetBuffer, (InputStream) nonNull, sink);
        } else if (nonNull instanceof Reader) {
            newPacket = writeReader(bindValue, packetBuffer, (Reader) nonNull, sink);
        } else if (nonNull instanceof char[]) {
            byte[] bytes = new String((char[]) nonNull).getBytes(this.clientCharset);
            writeByteEscapes(packetBuffer, bytes, bytes.length);
        } else if (nonNull instanceof ReadableByteChannel) {
            newPacket = writeChannel(bindValue, packetBuffer, (ReadableByteChannel) nonNull, sink);
        } else if (nonNull instanceof Path) {
            newPacket = writePath(packetBuffer, (Path) nonNull, sink);
        } else {
            throw BindUtils.createTypeNotMatchException(bindValue);
        }
        // 3. write quote char
        packetBuffer.writeByte(Constants.QUOTE_CHAR_BYTE);
        return newPacket;

    }

    /**
     * @see #bindToBytes(BindValue, ByteBuf, FluxSink)
     */
    private ByteBuf writePath(final ByteBuf packetBuffer
            , final Path path, final FluxSink<ByteBuf> sink) throws IOException {
        ByteBuf packet = packetBuffer;
        try (InputStream input = Files.newInputStream(path, StandardOpenOption.READ)) {
            final byte[] bufferArray = new byte[2048];
            int length;
            while ((length = input.read(bufferArray)) > 0) {

                writeByteEscapes(packetBuffer, bufferArray, length);

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
            return packet;
        } catch (IOException e) {
            packet.release();
            throw e;
        }


    }

    /**
     * @see #bindToBytes(BindValue, ByteBuf, FluxSink)
     */
    private ByteBuf writeChannel(final BindValue bindValue, final ByteBuf packetBuffer
            , final ReadableByteChannel channel, final FluxSink<ByteBuf> sink) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final byte[] bufferArray = new byte[2048];

            final ByteBuffer byteBuffer = ByteBuffer.wrap(bufferArray);

            while (channel.read(byteBuffer) > 0) {

                writeByteEscapes(packetBuffer, bufferArray, byteBuffer.remaining());

                byteBuffer.clear();
                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
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
            final CharBuffer charBuffer = CharBuffer.allocate(1024);
            final Charset clientCharset = this.clientCharset;
            ByteBuffer byteBuffer;
            byte[] bufferArray;
            while (reader.read(charBuffer) > 0) {//1. read char
                byteBuffer = clientCharset.encode(charBuffer); // 2. encode char'

                //3. write bytes with escapes.
                bufferArray = new byte[byteBuffer.remaining()];
                byteBuffer.get(bufferArray);
                writeByteEscapes(packetBuffer, bufferArray, bufferArray.length);

                charBuffer.clear();

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
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
            , final FluxSink<ByteBuf> sink) throws IOException {
        ByteBuf packet = packetBuffer;
        try {
            final byte[] bufferArray = new byte[2048];
            int length;
            while ((length = input.read(bufferArray)) > 0) {

                writeByteEscapes(packetBuffer, bufferArray, length);

                if (packet.readableBytes() >= PacketUtils.MAX_PACKET) {
                    packet = PacketUtils.publishAndCutBigPacket(packet, sink, this.sequenceIdSupplier
                            , this.adjutant::createByteBuffer);
                }
            }
            return packet;
        } catch (IOException e) {
            packet.release();
            throw e;
        } finally {
            if (this.properties.getOrDefault(PropertyKey.autoClosePStmtStreams, Boolean.class)) {
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


}
