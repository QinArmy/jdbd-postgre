package io.jdbd.mysql.protocol.client;

import io.jdbd.ReactiveSQLException;
import io.jdbd.ResultRow;
import io.jdbd.ResultRowMeta;
import io.jdbd.ResultStates;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.EofPacket;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.OkPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.netty.Connection;
import reactor.util.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static java.time.temporal.ChronoField.*;

abstract class AbstractClientProtocol implements ClientProtocol, ResultRowAdjutant {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractClientProtocol.class);


    static final DateTimeFormatter MYSQL_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)

            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 6, true)
            .toFormatter(Locale.ENGLISH);

    static final DateTimeFormatter MYSQL_DATETIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(MYSQL_TIME_FORMATTER)
            .toFormatter(Locale.ENGLISH);

    private static final long LONG_SIGNED_BIT = (1L << 63);


    final Connection connection;

    final MySQLUrl mySQLUrl;

    final MySQLCumulateReceiver cumulateReceiver;

    final Properties properties;

    private final Map<MySQLType, BiFunction<ByteBuf, MySQLColumnMeta, Object>> resultColumnParserMap = createResultColumnTypeParserMap();

    AbstractClientProtocol(Connection connection, MySQLUrl mySQLUrl, MySQLCumulateReceiver cumulateReceiver) {
        this.connection = connection;
        this.mySQLUrl = mySQLUrl;
        this.cumulateReceiver = cumulateReceiver;

        this.properties = mySQLUrl.getHosts().get(0).getProperties();
    }

    @Override
    public final Mono<Long> commandUpdate(String command, Consumer<ResultStates> statesConsumer) {
        return sendCommandPacket(command)
                .then(Mono.defer(() -> receiveCommandUpdatePacket(statesConsumer)));
    }


    @Override
    public final <T> Flux<T> commandQuery(String command, BiFunction<ResultRow, ResultRowMeta, T> rowDecoder
            , Consumer<ResultStates> statesConsumer) {
        return sendCommandPacket(command) //1. send COM_QUERY packet
                .then(Mono.defer(this::receiveResultSetMetadataPacket))//2. receive row meta packet
                // 3. receive rows and decode with rowDecoder
                .flatMapMany(rowMeta -> receiveResultSetRows(rowMeta, rowDecoder, statesConsumer))
                ;
    }

    @Override
    public final Mono<Void> closeGracefully() {
        // TODO optimize
        ByteBuf packetBuf = createPacketBuffer(1);
        packetBuf.writeByte(PacketUtils.COM_QUIT_HEADER);
        return sendPacket(packetBuf, null)
                .then();
    }

    public final ByteBuf createPacketBuffer(int initialPayloadCapacity) {
        return PacketUtils.createPacketBuffer(this.connection, initialPayloadCapacity);
    }



    /*################################## blow ResultRowAdjutant method ##################################*/


    final BiFunction<ByteBuf, MySQLColumnMeta, Object> obtainResultColumnParser(MySQLType mySQLType) {
        BiFunction<ByteBuf, MySQLColumnMeta, Object> function = this.resultColumnParserMap.get(mySQLType);
        if (function == null) {
            throw new JdbdMySQLException("Not found column parser for %s", mySQLType);
        }
        return function;
    }

    /**
     * @see #commandQuery(String, BiFunction, Consumer)
     * @see #commandUpdate(String, Consumer)
     */
    final Mono<Void> sendPacket(ByteBuf packetBuffer, @Nullable AtomicInteger sequenceId) {
        // TODO optimize packet send
        PacketUtils.writePacketHeader(packetBuffer, sequenceId == null ? 0 : sequenceId.addAndGet(1));
        return Mono.from(this.connection.outbound()
                .send(Mono.just(packetBuffer)));
    }


    abstract ZoneOffset obtainDatabaseZoneOffset();

    abstract int obtainNegotiatedCapability();

    abstract Charset obtainCharsetClient();

    /**
     * be equivalent to {@code (int)obtainCharsetClient().newEncoder().maxBytesPerChar()}
     */
    abstract int obtainMaxBytesPerCharClient();

    abstract Charset obtainCharsetResults();

    /**
     * @return a unmodifiable map
     */
    abstract Map<Integer, CharsetMapping.CustomCollation> obtainCustomCollationMap();

    /*################################## blow private method ##################################*/


    /**
     * @see MySQLType#DECIMAL
     * @see MySQLType#DECIMAL_UNSIGNED
     */
    @Nullable
    private BigDecimal parseDecimal(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String decimalText = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        try {
            return decimalText == null ? null : new BigDecimal(decimalText);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, decimalText);
        }
    }


    /**
     * @see MySQLType#TINYINT
     * @see MySQLType#TINYINT_UNSIGNED
     * @see MySQLType#SMALLINT
     * @see MySQLType#SMALLINT_UNSIGNED
     * @see MySQLType#MEDIUMINT
     * @see MySQLType#MEDIUMINT_UNSIGNED
     * @see MySQLType#INT
     */
    @Nullable
    private Integer parseInt(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String intText = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        try {
            return intText == null ? null : Integer.parseInt(intText);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, intText);
        }
    }

    /**
     * @see MySQLType#INT_UNSIGNED
     * @see MySQLType#BIGINT
     */
    @Nullable
    private Long parseLong(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String longText = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        try {
            return longText == null ? null : Long.parseLong(longText);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, longText);
        }
    }

    /**
     * @see MySQLType#BIGINT_UNSIGNED
     */
    @Nullable
    private BigInteger parseBigInteger(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String integerText = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        try {
            return integerText == null ? null : new BigInteger(integerText);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, integerText);
        }
    }

    /**
     * @see MySQLType#BOOLEAN
     */
    @Nullable
    private Boolean parseBoolean(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String booleanText = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        if (booleanText == null) {
            return null;
        }

        Boolean boolValue = MySQLStringUtils.tryConvertToBoolean(booleanText);
        if (boolValue != null) {
            return boolValue;
        }
        boolean value;
        try {
            int num = Integer.parseInt(booleanText);
            // Goes back to ODBC driver compatibility, and VB/Automation Languages/COM, where in Windows "-1" can mean true as well.
            value = num != 0;
        } catch (NumberFormatException e) {
            try {
                BigDecimal decimal = new BigDecimal(booleanText);
                // this means that 0.1 or -1 will be TRUE
                value = decimal.compareTo(BigDecimal.ZERO) != 0;
            } catch (Throwable exception) {
                throw createParserResultSetException(columnMeta, exception, booleanText);
            }
        }
        return value;
    }

    /**
     * @see MySQLType#FLOAT
     * @see MySQLType#FLOAT_UNSIGNED
     */
    @Nullable
    private Float parseFloat(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        if (text == null) {
            return null;
        }
        try {
            return Float.parseFloat(text);
        } catch (NumberFormatException e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#DOUBLE
     * @see MySQLType#DOUBLE_UNSIGNED
     */
    @Nullable
    private Double parseDouble(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        if (text == null) {
            return null;
        }
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#NULL
     */
    @Nullable
    private Object parseNull(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        // skip this column
        PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        return null;
    }

    /**
     * @see MySQLType#TIMESTAMP
     * @see MySQLType#DATETIME
     */
    @Nullable
    private LocalDateTime parseLocalDateTime(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        if (text == null) {
            return null;
        }
        try {
            // convert data zone to client zone
            return ZonedDateTime.of(LocalDateTime.parse(text, MYSQL_DATETIME_FORMATTER), obtainDatabaseZoneOffset())
                    .withZoneSameInstant(obtainClientZoneOffset())
                    .toLocalDateTime();
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#DATE
     */
    @Nullable
    private LocalDate parseLocalDate(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        if (text == null) {
            return null;
        }
        try {
            return LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }


    /**
     * @see MySQLType#TIME
     */
    @Nullable
    private LocalTime parseLocalTime(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        if (text == null) {
            return null;
        }
        try {
            return OffsetTime.of(LocalTime.parse(text, MYSQL_TIME_FORMATTER), obtainDatabaseZoneOffset())
                    .withOffsetSameInstant(obtainClientZoneOffset())
                    .toLocalTime();
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#YEAR
     */
    @Nullable
    private Year parseYear(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        if (text == null) {
            return null;
        }
        try {
            return Year.parse(text);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#CHAR
     * @see MySQLType#VARCHAR
     * @see MySQLType#JSON
     * @see MySQLType#ENUM
     * @see MySQLType#SET
     * @see MySQLType#TINYTEXT
     * @see MySQLType#MEDIUMTEXT
     * @see MySQLType#TEXT
     * @see MySQLType#LONGTEXT
     * @see MySQLType#UNKNOWN
     */
    @Nullable
    private String parseString(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        try {
            return PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, null);
        }
    }

    /**
     * @see MySQLType#BIT
     */
    @Nullable
    private Long parseLongForBit(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        String text = null;
        try {
            text = PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
            if (text == null) {
                return null;
            }
            boolean negative = text.length() == 64 && text.charAt(0) == '1';
            if (negative) {
                text = text.substring(1);
            }
            long bitResult = Long.parseLong(text, 2);
            if (negative) {
                bitResult |= LONG_SIGNED_BIT;
            }
            return bitResult;
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#BINARY
     * @see MySQLType#VARBINARY
     * @see MySQLType#TINYBLOB
     * @see MySQLType#BLOB
     * @see MySQLType#MEDIUMBLOB
     * @see MySQLType#LONGBLOB
     */
    @Nullable
    private byte[] parseByteArray(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        try {
            int len = PacketUtils.readInt1(multiRowBuf);
            if (len == PacketUtils.NULL_LENGTH) {
                return null;
            }
            byte[] bytes = new byte[len];
            multiRowBuf.readBytes(bytes);
            return bytes;
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, null);
        }
    }

    /**
     * @see MySQLType#GEOMETRY
     */
    @Nullable
    private Object parseGeometry(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        //TODO add Geometry class
        return PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
    }

    /**
     * @return a unmodifiable map.
     */
    private Map<MySQLType, BiFunction<ByteBuf, MySQLColumnMeta, Object>> createResultColumnTypeParserMap() {
        Map<MySQLType, BiFunction<ByteBuf, MySQLColumnMeta, Object>> map = new EnumMap<>(MySQLType.class);

        map.put(MySQLType.DECIMAL, this::parseDecimal);
        map.put(MySQLType.DECIMAL_UNSIGNED, this::parseDecimal);    // 1 fore
        map.put(MySQLType.TINYINT, this::parseInt);
        map.put(MySQLType.TINYINT_UNSIGNED, this::parseInt);

        map.put(MySQLType.BOOLEAN, this::parseBoolean);
        map.put(MySQLType.SMALLINT, this::parseInt);            // 2 fore
        map.put(MySQLType.SMALLINT_UNSIGNED, this::parseInt);
        map.put(MySQLType.INT, this::parseInt);

        map.put(MySQLType.INT_UNSIGNED, this::parseLong);
        map.put(MySQLType.FLOAT, this::parseFloat);           // 3 fore
        map.put(MySQLType.FLOAT_UNSIGNED, this::parseFloat);
        map.put(MySQLType.DOUBLE, this::parseDouble);

        map.put(MySQLType.DOUBLE_UNSIGNED, this::parseDouble);
        map.put(MySQLType.NULL, this::parseNull);          // 4 fore
        map.put(MySQLType.TIMESTAMP, this::parseLocalDateTime);
        map.put(MySQLType.BIGINT, this::parseLong);

        map.put(MySQLType.BIGINT_UNSIGNED, this::parseBigInteger);
        map.put(MySQLType.MEDIUMINT, this::parseInt);         // 5 fore
        map.put(MySQLType.MEDIUMINT_UNSIGNED, this::parseInt);
        map.put(MySQLType.DATE, this::parseLocalDate);

        map.put(MySQLType.TIME, this::parseLocalTime);
        map.put(MySQLType.DATETIME, this::parseLocalDateTime);        // 6 fore
        map.put(MySQLType.YEAR, this::parseYear);
        map.put(MySQLType.VARCHAR, this::parseString);

        map.put(MySQLType.VARBINARY, this::parseByteArray);
        map.put(MySQLType.BIT, this::parseLongForBit);        // 7 fore
        map.put(MySQLType.JSON, this::parseString);
        map.put(MySQLType.ENUM, this::parseString);

        map.put(MySQLType.SET, this::parseString);
        map.put(MySQLType.TINYBLOB, this::parseByteArray);        // 8 fore
        map.put(MySQLType.TINYTEXT, this::parseString);
        map.put(MySQLType.MEDIUMBLOB, this::parseByteArray);

        map.put(MySQLType.MEDIUMTEXT, this::parseString);
        map.put(MySQLType.LONGBLOB, this::parseByteArray);        // 9 fore
        map.put(MySQLType.LONGTEXT, this::parseString);
        map.put(MySQLType.BLOB, this::parseByteArray);

        map.put(MySQLType.TEXT, this::parseString);
        map.put(MySQLType.CHAR, this::parseString);        // 10 fore
        map.put(MySQLType.BINARY, this::parseByteArray);
        map.put(MySQLType.GEOMETRY, this::parseGeometry);

        map.put(MySQLType.UNKNOWN, this::parseString);

        return Collections.unmodifiableMap(map);
    }


    private Charset obtainResultColumnCharset(MySQLColumnMeta columnMeta) {
        Charset charset = CharsetMapping.getJavaCharsetByCollationIndex(columnMeta.collationIndex);
        if (charset == null) {
            Map<Integer, CharsetMapping.CustomCollation> map = obtainCustomCollationMap();
            CharsetMapping.CustomCollation collation = map.get(columnMeta.collationIndex);
            if (collation == null) {
                throw createNotFoundCustomCharsetException(columnMeta);
            }
            charset = CharsetMapping.getJavaCharsetByMySQLCharsetName(collation.charsetName);
            if (charset == null) {
                // here , io.jdbd.mysql.protocol.client.ClientConnectionProtocolImpl.detectCustomCollations have bugs.
                throw new IllegalStateException("Can't obtain ResultSet meta charset.");
            }
        }
        return charset;
    }


    /**
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     */
    private boolean textResultSetMultiRowDecoder(final ByteBuf cumulateBuf, FluxSink<ByteBuf> sink) {
        return PacketDecoders.textResultSetMultiRowDecoder(cumulateBuf, sink, obtainNegotiatedCapability());
    }

    /**
     * @see #commandQuery(String, BiFunction, Consumer)
     */
    private Mono<MySQLRowMeta> receiveResultSetMetadataPacket() {
        //  comQueryResultSetMetaDecoder will handle not expected packet,eg:ok,LOCAL INFILE Request .
        return this.cumulateReceiver.receiveOne(this::textResultSetMetaDecoder)
                .map(this::parseResultSetMetaPacket)
                ;
    }

    /**
     * @see #receiveResultSetMetadataPacket()
     */
    private boolean textResultSetMetaDecoder(final ByteBuf cumulateBuf, final MonoSink<ByteBuf> sink) {
        if (!PacketUtils.hasOnePacket(cumulateBuf)) {
            return false;
        }
        final int negotiatedCapability = obtainNegotiatedCapability();
        final PacketDecoders.ComQueryResponse responseType;
        responseType = PacketDecoders.detectComQueryResponseType(cumulateBuf, negotiatedCapability);
        boolean decodeEnd;
        switch (responseType) {
            case OK:
            case LOCAL_INFILE_REQUEST:
                // expected TEXT_RESULT, but OK/LOCAL_INFILE_REQUEST, read packet and drop.
                decodeEnd = PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK);//SEWAGE_SINK for drop
                if (decodeEnd) {
                    // notify downstream command sql not match
                    sink.error(MySQLExceptionUtils.createNonResultSetCommandException());
                }
                break;
            case ERROR:
                ErrorPacket packet;
                packet = PacketDecoders.decodeErrorPacket(cumulateBuf, negotiatedCapability, obtainCharsetResults());
                // notify downstream command sql error.
                sink.error(MySQLExceptionUtils.createErrorPacketException(packet));
                decodeEnd = true;
                break;
            case TEXT_RESULT:
                decodeEnd = PacketDecoders.textResultSetMetadataDecoder(cumulateBuf, sink, negotiatedCapability);
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(responseType);
        }
        return decodeEnd;
    }

    /**
     * @see #receiveResultSetMetadataPacket()
     */
    private MySQLRowMeta parseResultSetMetaPacket(ByteBuf rowMetaOrErrorPacket) {
        PacketDecoders.ComQueryResponse response;
        final int negotiatedCapability = obtainNegotiatedCapability();
        response = PacketDecoders.detectComQueryResponseType(rowMetaOrErrorPacket, negotiatedCapability);

        if (response == PacketDecoders.ComQueryResponse.TEXT_RESULT) {

            MySQLColumnMeta[] columnMetas = PacketDecoders.readResultColumnMetas(
                    rowMetaOrErrorPacket, negotiatedCapability
                    , obtainCharsetResults(), this.properties);
            return MySQLRowMeta.from(columnMetas, obtainCustomCollationMap());
        }
        throw new IllegalArgumentException("rowMetaOrErrorPacket error");

    }

    /**
     * @see #commandUpdate(String, Consumer)
     */
    private Mono<Long> receiveCommandUpdatePacket(Consumer<ResultStates> statesConsumer) {
        return this.cumulateReceiver.receiveOnePacket()
                .flatMap(packetBuf -> commandUpdateResultHandler(packetBuf, statesConsumer))
                ;
    }

    private enum ErrorType {
        TEXT_RESULT,
        LOCAL_INFILE_REQUEST
    }

    private abstract class AbstractCommandUpdateResultDecoder implements BiFunction<ByteBuf, MonoSink<ByteBuf>, Boolean> {

        //non-volatile ,because update in Netty EvenLoop .
        int textResultPhase = 0;

        private ErrorType errorType;

        @Override
        public final Boolean apply(ByteBuf cumulateBuf, MonoSink<ByteBuf> sink) {
            boolean decodeEnd = false;
            if (this.textResultPhase == 0) {
                // no error, command and Statement method match.
                decodeEnd = decodeResult(cumulateBuf, sink);
            }
            if (decodeEnd && this.textResultPhase == 0) {

            }
            // command and Statement method not match.
            switch (this.textResultPhase) {
                case 1:
                    if (!textResultSetMetaDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    if (!textResultSetMultiRowDecoder(cumulateBuf, PacketDecoders.SEWAGE_FLUX_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    if (!PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    break;
                case 2:
                    if (!textResultSetMultiRowDecoder(cumulateBuf, PacketDecoders.SEWAGE_FLUX_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    if (!PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    break;
                case 3:
                    if (!PacketDecoders.packetDecoder(cumulateBuf, PacketDecoders.SEWAGE_MONO_SINK)) {
                        break;
                    }
                    this.textResultPhase++;
                    break;
                default:
                    throw new IllegalStateException(
                            String.format("textResultPhase[%s] state error.", this.textResultPhase));
            }
            return this.textResultPhase > 3;
        }


        final void commandError(ErrorType errorType) {
            this.errorType = errorType;
        }

        abstract boolean decodeResult(ByteBuf byteBuf, MonoSink<ByteBuf> sink);

    }

    private final class CommandUpdateResultDecoder extends AbstractCommandUpdateResultDecoder {
        @Override
        boolean decodeResult(ByteBuf cumulateBuf, MonoSink<ByteBuf> sink) {
            if (!PacketUtils.hasOnePacket(cumulateBuf)) {
                return false;
            }
            final int negotiatedCapability = obtainNegotiatedCapability();
            final PacketDecoders.ComQueryResponse type;
            type = PacketDecoders.detectComQueryResponseType(cumulateBuf, negotiatedCapability);
            boolean decodeEnd;
            switch (type) {
                case ERROR:
                    ErrorPacket error;
                    error = PacketDecoders.decodeErrorPacket(cumulateBuf, negotiatedCapability, obtainCharsetResults());
                    sink.error(MySQLExceptionUtils.createErrorPacketException(error));
                    decodeEnd = true;
                    break;
                case OK:
                    decodeEnd = PacketDecoders.packetDecoder(cumulateBuf, sink);
                    break;
                case TEXT_RESULT:
                    this.commandError(ErrorType.TEXT_RESULT);
                    decodeEnd = false;
                    break;
                case LOCAL_INFILE_REQUEST:
                    this.commandError(ErrorType.LOCAL_INFILE_REQUEST);
                    decodeEnd = false;
                    break;
                default:
                    throw MySQLExceptionUtils.createUnknownEnumException(type);
            }
            return decodeEnd;
        }
    }

    private final class CommandLocalInfileRequestDecoder extends AbstractCommandUpdateResultDecoder {
        @Override
        boolean decodeResult(ByteBuf byteBuf, MonoSink<ByteBuf> sink) {
            return false;
        }
    }


    /**
     * @see #receiveCommandUpdatePacket(Consumer)
     */
    private Mono<Long> commandUpdateResultHandler(ByteBuf packetBuf, Consumer<ResultStates> statesConsumer) {
        packetBuf.skipBytes(3);
        if (packetBuf.readByte() != 1) {
            return Mono.error(new ReactiveSQLException(
                    new SQLException("Read COM_QUERY response error,sequence_id isn't 1.")));
        }

        if (ErrorPacket.isErrorPacket(packetBuf)) {
            ErrorPacket error = ErrorPacket.readPacket(packetBuf, obtainNegotiatedCapability(), obtainCharsetResults());
            return Mono.error(MySQLExceptionUtils.createErrorPacketException(error));
        }

        OkPacket okPacket = OkPacket.readPacket(packetBuf, obtainNegotiatedCapability());
        final ResultStates resultStates = MySQLResultStates.from(okPacket);
        return Mono.just(okPacket.getAffectedRows())
                .concatWith(Flux.defer(() -> commandUpdateStateConsume(resultStates, statesConsumer)))
                .elementAt(0)
                ;
    }

    /**
     * @return {@link Flux#empty()} or {@link Flux#error(Throwable)}
     */
    private Flux<Long> commandUpdateStateConsume(ResultStates resultStates, Consumer<ResultStates> statesConsumer) {
        Flux<Long> flux;
        try {
            statesConsumer.accept(resultStates);
            flux = Flux.empty();
        } catch (Throwable e) {
            flux = Flux.error(MySQLExceptionUtils.wrapExceptionIfNeed(e));
        }
        return flux;
    }


    /**
     * @see #commandQuery(String, BiFunction, Consumer)
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html">Protocol::Text Resultset</a>
     */
    private <T> Flux<T> receiveResultSetRows(MySQLRowMeta rowMeta
            , BiFunction<ResultRow, ResultRowMeta, T> rowDecoder, Consumer<ResultStates> statesConsumer) {
        return this.cumulateReceiver.receive(this::textResultSetMultiRowDecoder) // 1. decode packet,if error skip packet and publish error.
                .flatMap(multiRowBuf -> {
                    multiRowBuf.retain(); // for below convertResultRow method.
                    return Flux.<ResultRow>create(sink -> parseResultSetRows(sink, multiRowBuf, rowMeta));
                }) //2.parse ByteBuf to ResultRow
                .map(resultRow -> decodeResultRow(resultRow, rowMeta, rowDecoder)) //3. convert ResultRow to T
                .onErrorResume(this::handleResultSetTerminatorOnError) // optionally handle upstream error and terminator,finally publish error.
                .doOnCancel(() -> handleResultSetTerminator(statesConsumer).subscribe(v -> {
                }))
                .concatWith(Flux.defer(() -> handleResultSetTerminator(statesConsumer))) // 4.handle result set terminator,don't publish ResultRow
                ;
    }


    /**
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     */
    private <T> T decodeResultRow(ResultRow resultRow, MySQLRowMeta rowMeta
            , BiFunction<ResultRow, ResultRowMeta, T> rowDecoder) {
        T finalRow;
        try {
            finalRow = rowDecoder.apply(resultRow, rowMeta);
        } catch (Throwable e) {
            throw new ReactiveSQLException(new SQLException("rowDecoder throw exception.", e));
        }
        if (finalRow == null) {
            throw new ReactiveSQLException(new SQLException("Result Set decoder can't return null."));
        }
        return finalRow;
    }


    /**
     * @return {@link Flux#error(Throwable)}
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     */
    private <T> Flux<T> handleResultSetTerminatorOnError(Throwable e) {
        return handleResultSetTerminator(EMPTY_STATE_CONSUMER)
                .thenMany(Flux.error(MySQLExceptionUtils.wrapExceptionIfNeed(e)))
                ;
    }


    /**
     * @return {@link Flux#empty()} or {@link Flux#error(Throwable)} (if terminator is error packet.)
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     * @see #handleResultSetTerminatorOnError(Throwable)
     */
    private <T> Flux<T> handleResultSetTerminator(Consumer<ResultStates> statesConsumer) {
        // use packet decoder , because Text ResultRow decoder do it.
        return this.cumulateReceiver.receiveOnePacket()
                .flatMapMany(packetBuf -> handleResultSetTerminator0(packetBuf, statesConsumer))
                ;
    }

    /**
     * handle Text ResultSet terminator.
     *
     * @return {@link Flux#empty()} or {@link Flux#error(Throwable)} (if terminator is error packet.)
     * @see #handleResultSetTerminator(Consumer)
     */
    private <T> Flux<T> handleResultSetTerminator0(ByteBuf terminatorPacket
            , Consumer<ResultStates> statesConsumer) {
        final int payloadLength = PacketUtils.readInt3(terminatorPacket);
        terminatorPacket.skipBytes(1);// skip sequence_id

        final int negotiatedCapability = obtainNegotiatedCapability();
        final boolean clientDeprecateEof = (negotiatedCapability & ClientProtocol.CLIENT_DEPRECATE_EOF) != 0;
        Flux<T> flux;
        switch (PacketUtils.getInt1(terminatorPacket, terminatorPacket.readerIndex())) {
            case ErrorPacket.ERROR_HEADER:
                // ERROR terminator
                ErrorPacket error;
                error = ErrorPacket.readPacket(terminatorPacket, negotiatedCapability, obtainCharsetResults());
                flux = handleResultSetErrorPacket(error);
                break;
            case EofPacket.EOF_HEADER:
                if (clientDeprecateEof && payloadLength < PacketUtils.ENC_3_MAX_VALUE) {
                    //OK terminator
                    OkPacket okPacket = OkPacket.readPacket(terminatorPacket, negotiatedCapability);
                    flux = textResultSetTerminatorConsumer(MySQLResultStates.from(okPacket), statesConsumer);
                    break;
                } else if (!clientDeprecateEof && payloadLength < 6) {
                    // EOF terminator
                    EofPacket eofPacket = EofPacket.readPacket(terminatorPacket, negotiatedCapability);
                    flux = textResultSetTerminatorConsumer(MySQLResultStates.from(eofPacket), statesConsumer);
                    break;
                }
            default:
                // never here ,if Text ResultSet row decoder no bug.
                flux = Flux.error(new ReactiveSQLException(new SQLException("Text ResultSet row decoder error.")));

        }
        return flux;
    }

    /**
     * @return {@link Flux#error(Throwable)}
     * @see #handleResultSetTerminator0(ByteBuf, Consumer)
     */
    private <T> Flux<T> handleResultSetErrorPacket(ErrorPacket error) {
        return Flux.error(new ReactiveSQLException(
                new SQLException(error.getErrorMessage(), error.getSqlState())));
    }


    /**
     * @see #receiveResultSetRows(MySQLRowMeta, BiFunction, Consumer)
     */
    private void parseResultSetRows(FluxSink<ResultRow> sink, ByteBuf multiRowBuf, MySQLRowMeta metadata) {
        final MySQLColumnMeta[] columnMetas = metadata.columnMetas;
        try {
            int payloadLength, payloadIndex;
            while (multiRowBuf.isReadable()) {

                payloadLength = PacketUtils.readInt3(multiRowBuf);
                multiRowBuf.skipBytes(1); // skip sequence id

                payloadIndex = multiRowBuf.readerIndex();

                Object[] columnValues = new Object[columnMetas.length];
                MySQLColumnMeta columnMeta;
                for (int i = 0; i < columnMetas.length; i++) {
                    columnMeta = columnMetas[i];
                    columnValues[i] = obtainResultColumnParser(columnMeta.mysqlType).apply(multiRowBuf, columnMeta);
                }
                multiRowBuf.readerIndex(payloadIndex + payloadLength); // to next pakcet
                sink.next(MySQLResultRow.from(columnValues, metadata, this));
            }
            sink.complete();
        } catch (Throwable e) {
            LOG.error("Parse text ResultSet rows error.", e);
            sink.error(e);
        } finally {
            multiRowBuf.release();
        }

    }

    /**
     * @see #commandUpdate(String, Consumer)
     * @see #commandQuery(String, BiFunction, Consumer)
     */
    private Mono<Void> sendCommandPacket(String command) {
        // 1. create COM_QUERY packet.
        ByteBuf packetBuf = createPacketBuffer(command.length() * obtainMaxBytesPerCharClient());
        packetBuf.writeByte(PacketUtils.COM_QUERY_HEADER)
                .writeBytes(command.getBytes(obtainCharsetClient()));
        return sendPacket(packetBuf, null);
    }


    /*################################## blow private static method  ##################################*/

    /**
     * @see #parseDecimal(ByteBuf, MySQLColumnMeta)
     */
    private static ReactiveSQLException createParserResultSetException(MySQLColumnMeta columnMeta, Throwable e
            , @Nullable String textValue) {
        // here ,1.maybe parse code error; 2.maybe server send packet error.
        StringBuilder builder = new StringBuilder("Cannot parse");
        if (textValue != null) {
            builder.append("[")
                    .append(textValue)
                    .append("]")
            ;
        }
        appendColumnDetailForSQLException(builder, columnMeta);

        return new ReactiveSQLException(new SQLException(builder.toString(), e));
    }


    private static ReactiveSQLException createNotFoundCustomCharsetException(MySQLColumnMeta columnMeta) {
        // to here , code error,because check after load custom charset.
        StringBuilder builder = new StringBuilder("Not found java charset for");
        appendColumnDetailForSQLException(builder, columnMeta);
        builder.append(" ,Collation Index[")
                .append(columnMeta.collationIndex)
                .append("].");
        return new ReactiveSQLException(new SQLException(builder.toString()));

    }

    private static void appendColumnDetailForSQLException(StringBuilder builder, MySQLColumnMeta columnMeta) {
        if (MySQLStringUtils.hasText(columnMeta.tableName)) {
            builder.append(" TableName[")
                    .append(columnMeta.tableName)
                    .append("]");
        }
        if (MySQLStringUtils.hasText(columnMeta.tableAlias)) {
            builder.append(" TableAlias[")
                    .append(columnMeta.tableAlias)
                    .append("]");
        }
        if (MySQLStringUtils.hasText(columnMeta.columnName)) {
            builder.append(" ColumnName[")
                    .append(columnMeta.columnName)
                    .append("]");
        }
        if (MySQLStringUtils.hasText(columnMeta.columnAlias)) {
            builder.append(" ColumnAlias[")
                    .append(columnMeta.columnAlias)
                    .append("]");
        }
    }

    private static <T> Flux<T> textResultSetTerminatorConsumer(ResultStates resultStates
            , Consumer<ResultStates> statesConsumer) {
        Flux<T> flux;
        try {
            statesConsumer.accept(resultStates);
            flux = Flux.empty();
        } catch (Throwable e) {
            flux = Flux.error(new ReactiveSQLException(new SQLException("statesConsumer throw exception.", e)));
        }
        return flux;
    }


}
