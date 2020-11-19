package io.jdbd.mysql.protocol.client;

import io.jdbd.ReactiveSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.type.Geometry;
import io.jdbd.mysql.util.MySQLExceptionUtils;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.jdbd.mysql.util.MySQLTimeUtils;
import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Map;

abstract class ColumnParsers {

    protected ColumnParsers() {
        throw new UnsupportedOperationException();
    }


    private static final long LONG_SIGNED_BIT = (1L << 63);

    @Nullable
    static Object parseColumn(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, StatementTaskAdjutant adjutant) {
        final MySQLType sqlType = columnMeta.mysqlType;
        final Charset columnCharset = obtainResultColumnCharset(columnMeta, adjutant);
        Object value;
        switch (sqlType) {
            // more probability
            case NULL:
                value = parseNull(multiRowBuf, columnCharset);
                break;
            case INT:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case TINYINT:
            case TINYINT_UNSIGNED:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                value = parseInt(multiRowBuf, columnMeta, columnCharset);
                break;
            case BIGINT:
            case INT_UNSIGNED:
                value = parseLong(multiRowBuf, columnMeta, columnCharset);
                break;
            case VARCHAR:
            case CHAR:
            case JSON:
            case SET:
            case ENUM:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                value = parseString(multiRowBuf, columnMeta, columnCharset);
                break;
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                value = parseDecimal(multiRowBuf, columnMeta, columnCharset);
                break;
            case DATETIME:
            case TIMESTAMP:
                value = parseLocalDateTime(multiRowBuf, columnMeta, columnCharset
                        , adjutant.obtainZoneOffsetDatabase(), adjutant.obtainZoneOffsetClient());
                break;
            case DATE:
                value = parseLocalDate(multiRowBuf, columnMeta, columnCharset);
                break;
            case TIME:
                value = parseLocalTime(multiRowBuf, columnMeta, columnCharset
                        , adjutant.obtainZoneOffsetDatabase(), adjutant.obtainZoneOffsetClient());
                break;
            case YEAR:
                value = parseYear(multiRowBuf, columnMeta, columnCharset);
                break;
            case BIT:
                value = parseLongForBit(multiRowBuf, columnMeta, columnCharset);
                break;
            case BOOLEAN:
                value = parseBoolean(multiRowBuf, columnMeta, columnCharset);
                break;
            case FLOAT:
            case FLOAT_UNSIGNED:
                value = parseFloat(multiRowBuf, columnMeta, columnCharset);
                break;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                value = parseDouble(multiRowBuf, columnMeta, columnCharset);
                break;
            case BIGINT_UNSIGNED:
                value = parseBigInteger(multiRowBuf, columnMeta, columnCharset);
                break;
            case TINYBLOB:
            case MEDIUMBLOB:
            case BLOB:
            case LONGBLOB:

            case BINARY:
            case VARBINARY:
                value = parseByteArray(multiRowBuf, columnMeta);
                break;
            case GEOMETRY:
                value = parseGeometry(multiRowBuf, columnMeta, columnCharset);
                break;
            case UNKNOWN:
                value = parseUnknown(multiRowBuf, columnMeta, columnCharset);
                break;
            default:
                throw MySQLExceptionUtils.createUnknownEnumException(sqlType);
        }
        return value;
    }


    /**
     * @see MySQLType#DECIMAL
     * @see MySQLType#DECIMAL_UNSIGNED
     */
    @Nullable
    private static BigDecimal parseDecimal(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String decimalText = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static Integer parseInt(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String intText = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static Long parseLong(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String longText = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static BigInteger parseBigInteger(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String integerText = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static Boolean parseBoolean(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String booleanText = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static Float parseFloat(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static Double parseDouble(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static Object parseNull(ByteBuf multiRowBuf, Charset charsetResult) {
        // skip this column
        PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
        return null;
    }

    /**
     * @see MySQLType#TIMESTAMP
     * @see MySQLType#DATETIME
     */
    @Nullable
    private static LocalDateTime parseLocalDateTime(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta
            , Charset charsetResult, ZoneOffset zoneOffsetDatabase, ZoneOffset zoneOffsetClient) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
        if (text == null) {
            return null;
        }
        try {
            // convert data zone to client zone
            return OffsetDateTime.of(LocalDateTime.parse(text, MySQLTimeUtils.MYSQL_DATETIME_FORMATTER), zoneOffsetDatabase)
                    .withOffsetSameInstant(zoneOffsetClient)
                    .toLocalDateTime();
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#DATE
     */
    @Nullable
    private static LocalDate parseLocalDate(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static LocalTime parseLocalTime(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult
            , ZoneOffset zoneOffsetDatabase, ZoneOffset zoneOffsetClient) {

        String text = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
        if (text == null) {
            return null;
        }
        try {
            return OffsetTime.of(LocalTime.parse(text, MySQLTimeUtils.MYSQL_TIME_FORMATTER), zoneOffsetDatabase)
                    .withOffsetSameInstant(zoneOffsetClient)
                    .toLocalTime();
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, text);
        }
    }

    /**
     * @see MySQLType#YEAR
     */
    @Nullable
    private static Year parseYear(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String text = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static String parseString(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        try {
            return PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, null);
        }
    }

    /**
     * @see MySQLType#BIT
     */
    @Nullable
    private static Long parseLongForBit(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        String text = null;
        try {
            text = PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
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
    private static byte[] parseByteArray(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta) {
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
    private static Geometry parseGeometry(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        //TODO add Geometry class
        throw new UnsupportedOperationException();
      /*  try {
            return PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, null);
        }*/
    }

    /**
     * @see MySQLType#UNKNOWN
     */
    @Nullable
    private static Object parseUnknown(ByteBuf multiRowBuf, MySQLColumnMeta columnMeta, Charset charsetResult) {
        //TODO add UNKNOWN class
        try {
            return PacketUtils.readStringLenEnc(multiRowBuf, charsetResult);
        } catch (Throwable e) {
            throw createParserResultSetException(columnMeta, e, null);
        }
    }

    /**
     * @see #parseDecimal(ByteBuf, MySQLColumnMeta, Charset)
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

    private static Charset obtainResultColumnCharset(MySQLColumnMeta columnMeta, StatementTaskAdjutant adjutant) {
        Charset charset = CharsetMapping.getJavaCharsetByCollationIndex(columnMeta.collationIndex);
        if (charset == null) {
            Map<Integer, CharsetMapping.CustomCollation> map = adjutant.obtainCustomCollationMap();
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

    private static ReactiveSQLException createNotFoundCustomCharsetException(MySQLColumnMeta columnMeta) {
        // to here , code error,because check after load custom charset.
        StringBuilder builder = new StringBuilder("Not found java charset for");
        appendColumnDetailForSQLException(builder, columnMeta);
        builder.append(" ,Collation Index[")
                .append(columnMeta.collationIndex)
                .append("].");
        return new ReactiveSQLException(new SQLException(builder.toString()));

    }


}
