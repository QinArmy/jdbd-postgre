package io.jdbd.mysql.protocol.client;

import io.jdbd.ReactiveSQLException;
import io.jdbd.ResultRow;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.netty.buffer.ByteBuf;
import org.qinarmy.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
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
import java.util.function.BiFunction;

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

    private final Map<MySQLType, BiFunction<ByteBuf, MySQLColumnMeta, Object>> resultColumnParserMap = createResultColumnTypeParserMap();

    AbstractClientProtocol(Connection connection, MySQLUrl mySQLUrl, MySQLCumulateReceiver cumulateReceiver) {
        this.connection = connection;
        this.mySQLUrl = mySQLUrl;
        this.cumulateReceiver = cumulateReceiver;
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
     * receive multi row and read terminator, if error, publish error.
     *
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html">Protocol::Text Resultset</a>
     */
    final Flux<ResultRow> receiveSingleResultSetRowDataAndHandleTerminator(MySQLRowMeta rowMeta) {
        return this.cumulateReceiver.receive(PacketDecoders::resultSetMultiRowDecoder)
                .flatMap(multiRowBuf -> {
                    multiRowBuf.retain(); // for below convertResultRow method.
                    return Flux.<ResultRow>create(sink -> convertResultRow(sink, multiRowBuf, rowMeta));
                })
                .onErrorResume(this::handleSingleResultTerminatorOnError)
                .concatWith(Mono.defer(this::readAndHandleSingleResultTerminator))
                ;
    }


    abstract Map<Integer, Charset> obtainCustomCollationIndexToCharsetMap();

    abstract Map<Integer, Integer> obtainCustomCollationIndexToMblenMap();

    abstract ZoneOffset obtainDatabaseZoneOffset();

    abstract int obtainNegotiatedCapability();

    abstract Charset obtainCharsetClient();

    abstract Charset obtainCharsetResults();

    /*################################## blow private method ##################################*/


    /**
     * @see MySQLType#DECIMAL
     * @see MySQLType#DECIMAL_UNSIGNED
     */
    @Nullable
    private BigDecimal toDecimal(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Integer toInt(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Long toLong(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private BigInteger toBigInteger(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Boolean toBoolean(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Float toFlat(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Double toDouble(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Object toNull(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        // skip this column
        PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
        return null;
    }

    /**
     * @see MySQLType#TIMESTAMP
     * @see MySQLType#DATETIME
     */
    @Nullable
    private LocalDateTime toLocalDateTime(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private LocalDate toLocalDate(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private LocalTime toLocalTime(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Year toYear(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private String toString(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Long toLongForBit(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private byte[] toByteArray(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private Object toGeometry(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
        //TODO add Geometry class
        return PacketUtils.readStringLenEnc(multiRowBuf, obtainResultColumnCharset(columnMeta));
    }

    /**
     * @return a unmodifiable map.
     */
    private Map<MySQLType, BiFunction<ByteBuf, MySQLColumnMeta, Object>> createResultColumnTypeParserMap() {
        Map<MySQLType, BiFunction<ByteBuf, MySQLColumnMeta, Object>> map = new EnumMap<>(MySQLType.class);

        map.put(MySQLType.DECIMAL, this::toDecimal);
        map.put(MySQLType.DECIMAL_UNSIGNED, this::toDecimal);    // 1 fore
        map.put(MySQLType.TINYINT, this::toInt);
        map.put(MySQLType.TINYINT_UNSIGNED, this::toInt);

        map.put(MySQLType.BOOLEAN, this::toBoolean);
        map.put(MySQLType.SMALLINT, this::toInt);            // 2 fore
        map.put(MySQLType.SMALLINT_UNSIGNED, this::toInt);
        map.put(MySQLType.INT, this::toInt);

        map.put(MySQLType.INT_UNSIGNED, this::toLong);
        map.put(MySQLType.FLOAT, this::toFlat);           // 3 fore
        map.put(MySQLType.FLOAT_UNSIGNED, this::toFlat);
        map.put(MySQLType.DOUBLE, this::toDouble);

        map.put(MySQLType.DOUBLE_UNSIGNED, this::toDouble);
        map.put(MySQLType.NULL, this::toNull);          // 4 fore
        map.put(MySQLType.TIMESTAMP, this::toLocalDateTime);
        map.put(MySQLType.BIGINT, this::toLong);

        map.put(MySQLType.BIGINT_UNSIGNED, this::toBigInteger);
        map.put(MySQLType.MEDIUMINT, this::toInt);         // 5 fore
        map.put(MySQLType.MEDIUMINT_UNSIGNED, this::toInt);
        map.put(MySQLType.DATE, this::toLocalDate);

        map.put(MySQLType.TIME, this::toLocalTime);
        map.put(MySQLType.DATETIME, this::toLocalDateTime);        // 6 fore
        map.put(MySQLType.YEAR, this::toYear);
        map.put(MySQLType.VARCHAR, this::toString);

        map.put(MySQLType.VARBINARY, this::toByteArray);
        map.put(MySQLType.BIT, this::toLongForBit);        // 7 fore
        map.put(MySQLType.JSON, this::toString);
        map.put(MySQLType.ENUM, this::toString);

        map.put(MySQLType.SET, this::toString);
        map.put(MySQLType.TINYBLOB, this::toByteArray);        // 8 fore
        map.put(MySQLType.TINYTEXT, this::toString);
        map.put(MySQLType.MEDIUMBLOB, this::toByteArray);

        map.put(MySQLType.MEDIUMTEXT, this::toString);
        map.put(MySQLType.LONGBLOB, this::toByteArray);        // 9 fore
        map.put(MySQLType.LONGTEXT, this::toString);
        map.put(MySQLType.BLOB, this::toByteArray);

        map.put(MySQLType.TEXT, this::toString);
        map.put(MySQLType.CHAR, this::toString);        // 10 fore
        map.put(MySQLType.BINARY, this::toByteArray);
        map.put(MySQLType.GEOMETRY, this::toGeometry);

        map.put(MySQLType.UNKNOWN, this::toString);

        return Collections.unmodifiableMap(map);
    }


    private Charset obtainResultColumnCharset(MySQLColumnMeta columnMeta) {
        Charset charset = CharsetMapping.getJavaCharsetByCollationIndex(columnMeta.collationIndex);
        if (charset == null) {
            Map<Integer, Charset> customCharset = obtainCustomCollationIndexToCharsetMap();
            charset = customCharset.get(columnMeta.collationIndex);
            if (charset == null) {
                throw createNotFoundCustomCharsetException(columnMeta);
            }
        }
        return charset;
    }


    private void convertResultRow(FluxSink<ResultRow> sink, ByteBuf multiRowBuf, MySQLRowMeta metadata) {
        final MySQLColumnMeta[] columnMetas = metadata.columnMetas;

        try {
            while (multiRowBuf.isReadable()) {
                multiRowBuf.skipBytes(PacketUtils.HEADER_SIZE);
                Object[] columnValues = new Object[columnMetas.length];
                MySQLColumnMeta columnMeta;
                for (int i = 0; i < columnMetas.length; i++) {
                    columnMeta = columnMetas[i];
                    columnValues[i] = obtainResultColumnParser(columnMeta.mysqlType).apply(multiRowBuf, columnMeta);
                }
                sink.next(MySQLResultRow.from(columnValues, metadata, this));
            }
            sink.complete();
        } catch (Throwable e) {
            sink.error(e);
        } finally {
            multiRowBuf.release();
        }

    }

    /**
     * @see #readAndHandleSingleResultTerminator()
     */
    private Mono<ResultRow> handleSingleResultTerminatorOnError(Throwable e) {
        return this.cumulateReceiver.receiveOnePacket() // skip terminator packet
                .doOnNext(byteBuf -> LOG.debug("handle single result terminator finish on error."))
                .then(Mono.error(e))
                ;
    }

    /**
     * @see #handleSingleResultTerminatorOnError(Throwable)
     */
    private Mono<ResultRow> readAndHandleSingleResultTerminator() {
        return this.cumulateReceiver.receiveOnePacket()
                .flatMap(this::handleSingleResultSetTerminator)
                .then(Mono.empty())
                ;
    }

    private Mono<ResultRow> handleSingleResultSetTerminator(ByteBuf terminatorPacket) {
        terminatorPacket.skipBytes(PacketUtils.HEADER_SIZE); // skip header
        Mono<ResultRow> mono;
        if (ErrorPacket.isErrorPacket(terminatorPacket)) {
            ErrorPacket packet = ErrorPacket.readPacket(terminatorPacket
                    , obtainNegotiatedCapability(), obtainCharsetResults());
            mono = Mono.error(new ReactiveSQLException(
                    new SQLException(packet.getErrorMessage(), packet.getSqlState())));
        } else {
            mono = Mono.empty();
        }
        return mono;
    }

    /*################################## blow private static method  ##################################*/

    /**
     * @see #toDecimal(ByteBuf, MySQLColumnMeta)
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
        if (StringUtils.hasText(columnMeta.tableName)) {
            builder.append(" TableName[")
                    .append(columnMeta.tableName)
                    .append("]");
        }
        if (StringUtils.hasText(columnMeta.tableAlias)) {
            builder.append(" TableAlias[")
                    .append(columnMeta.tableAlias)
                    .append("]");
        }
        if (StringUtils.hasText(columnMeta.columnName)) {
            builder.append(" ColumnName[")
                    .append(columnMeta.columnName)
                    .append("]");
        }
        if (StringUtils.hasText(columnMeta.columnAlias)) {
            builder.append(" ColumnAlias[")
                    .append(columnMeta.columnAlias)
                    .append("]");
        }
    }


}
