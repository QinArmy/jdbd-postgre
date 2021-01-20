package io.jdbd.mysql.protocol.client;

import io.jdbd.meta.SQLType;
import io.jdbd.mysql.protocol.CharsetMapping;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLObjects;
import io.jdbd.type.Geometry;
import org.qinarmy.util.StringUtils;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.JDBCType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.*;
import java.util.function.BiFunction;

/**
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/data-types.html"> Data Types</a>
 */
public enum MySQLType implements SQLType {

    /**
     * DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]
     * A packed "exact" fixed-point number. M is the total number of digits (the precision) and D is the number of digits
     * after the decimal point (the scale). The decimal point and (for negative numbers) the "-" sign are not counted in M.
     * If D is 0, values have no decimal point or fractional part. The maximum number of digits (M) for DECIMAL is 65.
     * The maximum number of supported decimals (D) is 30. If D is omitted, the default is 0. If M is omitted, the default is 10.
     * <p>
     * Protocol: TYPE_DECIMAL = 0
     * Protocol: TYPE_NEWDECIMAL = 246
     * <p>
     * These types are synonyms for DECIMAL:
     * DEC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL],
     * FIXED[(M[,D])] [UNSIGNED] [ZEROFILL]
     */
    DECIMAL(Constants.TYPE_DECIMAL, JDBCType.DECIMAL, BigDecimal.class),
    /**
     * DECIMAL[(M[,D])] UNSIGNED [ZEROFILL]
     *
     * @see #DECIMAL
     */
    DECIMAL_UNSIGNED(Constants.TYPE_DECIMAL, true, JDBCType.DECIMAL, BigDecimal.class),
    /**
     * TINYINT[(M)] [UNSIGNED] [ZEROFILL]
     * A very small integer. The signed range is -128 to 127. The unsigned range is 0 to 255.
     * <p>
     * Protocol: TYPE_TINY = 1
     */
    TINYINT(Constants.TYPE_TINY, JDBCType.TINYINT, Integer.class),
    /**
     * TINYINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #TINYINT
     */
    TINYINT_UNSIGNED(Constants.TYPE_TINY, true, JDBCType.TINYINT, Integer.class),
    /**
     * BOOL, BOOLEAN
     * These types are synonyms for TINYINT(1). A value of zero is considered false. Nonzero values are considered true
     * <p>
     * BOOLEAN is converted to TINYINT(1) during DDL execution i.e. it has the same precision=3. Thus we have to
     * look at full data type name and convert TINYINT to BOOLEAN (or BIT) if it has "(1)" length specification.
     * <p>
     * Protocol: TYPE_TINY = 1
     */
    BOOLEAN(Constants.TYPE_BOOL, JDBCType.BOOLEAN, Boolean.class),
    /**
     * SMALLINT[(M)] [UNSIGNED] [ZEROFILL]
     * A small integer. The signed range is -32768 to 32767. The unsigned range is 0 to 65535.
     * <p>
     * Protocol: TYPE_SHORT = 2
     */
    SMALLINT(Constants.TYPE_SHORT, JDBCType.SMALLINT, Integer.class),
    /**
     * SMALLINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #SMALLINT
     */
    SMALLINT_UNSIGNED(Constants.TYPE_SHORT, true, JDBCType.SMALLINT, Integer.class),
    /**
     * INT[(M)] [UNSIGNED] [ZEROFILL]
     * A normal-size integer. The signed range is -2147483648 to 2147483647. The unsigned range is 0 to 4294967295.
     * <p>
     * Protocol: TYPE_LONG = 3
     * <p>
     * INTEGER[(M)] [UNSIGNED] [ZEROFILL] is a synonym for INT.
     */
    INT(Constants.TYPE_LONG, JDBCType.INTEGER, Integer.class),
    /**
     * INT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #INT
     */
    INT_UNSIGNED(Constants.TYPE_LONG, true, JDBCType.INTEGER, Long.class),
    /**
     * FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]
     * A small (single-precision) floating-point number. Permissible values are -3.402823466E+38 to -1.175494351E-38, 0,
     * and 1.175494351E-38 to 3.402823466E+38. These are the theoretical limits, based on the IEEE standard. The actual
     * range might be slightly smaller depending on your hardware or operating system.
     * <p>
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A single-precision floating-point number is accurate to
     * approximately 7 decimal places.
     * <p>
     * Protocol: TYPE_FLOAT = 4
     * <p>
     * Additionally:
     * FLOAT(p) [UNSIGNED] [ZEROFILL]
     * A floating-point number. p represents the precision in bits, but MySQL uses this value only to determine whether
     * to use FLOAT or DOUBLE for the resulting data type. If p is from 0 to 24, the data type becomes FLOAT with no M or D values.
     * If p is from 25 to 53, the data type becomes DOUBLE with no M or D values. The range of the resulting column is the same as
     * for the single-precision FLOAT or double-precision DOUBLE data types.
     */
    FLOAT(Constants.TYPE_FLOAT, JDBCType.REAL, Float.class),
    /**
     * FLOAT[(M,D)] UNSIGNED [ZEROFILL]
     *
     * @see #FLOAT
     */
    FLOAT_UNSIGNED(Constants.TYPE_FLOAT, true, JDBCType.REAL, Float.class),

    /**
     * DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]
     * A normal-size (double-precision) floating-point number. Permissible values are -1.7976931348623157E+308 to
     * -2.2250738585072014E-308, 0, and 2.2250738585072014E-308 to 1.7976931348623157E+308. These are the theoretical limits,
     * based on the IEEE standard. The actual range might be slightly smaller depending on your hardware or operating system.
     * <p>
     * M is the total number of digits and D is the number of digits following the decimal point. If M and D are omitted,
     * values are stored to the limits permitted by the hardware. A double-precision floating-point number is accurate to
     * approximately 15 decimal places.
     * <p>
     * Protocol: TYPE_DOUBLE = 5
     * <p>
     * These types are synonyms for DOUBLE:
     * DOUBLE PRECISION[(M,D)] [UNSIGNED] [ZEROFILL],
     * REAL[(M,D)] [UNSIGNED] [ZEROFILL]. Exception: If the REAL_AS_FLOAT SQL mode is enabled, REAL is a synonym for FLOAT rather than DOUBLE.
     */
    DOUBLE(Constants.TYPE_DOUBLE, JDBCType.DOUBLE, Double.class),
    /**
     * DOUBLE[(M,D)] UNSIGNED [ZEROFILL]
     *
     * @see #DOUBLE
     */
    DOUBLE_UNSIGNED(Constants.TYPE_DOUBLE, true, JDBCType.DOUBLE, Double.class),
    /**
     * TYPE_NULL = 6
     */
    NULL(Constants.TYPE_NULL, JDBCType.NULL, Object.class),
    /**
     * TIMESTAMP[(fsp)]
     * A timestamp. The range is '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
     * TIMESTAMP values are stored as the number of seconds since the epoch ('1970-01-01 00:00:00' UTC).
     * A TIMESTAMP cannot represent the value '1970-01-01 00:00:00' because that is equivalent to 0 seconds
     * from the epoch and the value 0 is reserved for representing '0000-00-00 00:00:00', the "zero" TIMESTAMP value.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * <p>
     * Protocol: TYPE_TIMESTAMP = 7
     */
    // TODO If MySQL server run with the MAXDB SQL mode enabled, TIMESTAMP is identical with DATETIME. If this mode is enabled at the time that a table is created, TIMESTAMP columns are created as DATETIME columns.
    // As a result, such columns use DATETIME display format, have the same range of values, and there is no automatic initialization or updating to the current date and time
    TIMESTAMP(Constants.TYPE_TIMESTAMP, JDBCType.TIMESTAMP, LocalDateTime.class),

    /**
     * BIGINT[(M)] [UNSIGNED] [ZEROFILL]
     * A large integer. The signed range is -9223372036854775808 to 9223372036854775807. The unsigned range is 0 to 18446744073709551615.
     * <p>
     * Protocol: TYPE_LONGLONG = 8
     * <p>
     * SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
     */
    BIGINT(Constants.TYPE_LONGLONG, JDBCType.BIGINT, Long.class),
    /**
     * BIGINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #BIGINT
     */
    BIGINT_UNSIGNED(Constants.TYPE_LONGLONG, true, JDBCType.BIGINT, BigInteger.class),
    /**
     * MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]
     * A medium-sized integer. The signed range is -8388608 to 8388607. The unsigned range is 0 to 16777215.
     * <p>
     * Protocol: TYPE_INT24 = 9
     */
    MEDIUMINT(Constants.TYPE_INT24, JDBCType.INTEGER, Integer.class),
    /**
     * MEDIUMINT[(M)] UNSIGNED [ZEROFILL]
     *
     * @see #MEDIUMINT
     */
    MEDIUMINT_UNSIGNED(Constants.TYPE_INT24, true, JDBCType.INTEGER, Integer.class),
    /**
     * DATE
     * A date. The supported range is '1000-01-01' to '9999-12-31'. MySQL displays DATE values in 'YYYY-MM-DD' format,
     * but permits assignment of values to DATE columns using either strings or numbers.
     * <p>
     * Protocol: TYPE_DATE = 10
     */
    DATE(Constants.TYPE_DATE, JDBCType.DATE, LocalDate.class),
    /**
     * TIME[(fsp)]
     * A time. The range is '-838:59:59.000000' to '838:59:59.000000'. MySQL displays TIME values in
     * 'HH:MM:SS[.fraction]' format, but permits assignment of values to TIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * <p>
     * Protocol: TYPE_TIME = 11
     */
    TIME(Constants.TYPE_TIME, JDBCType.TIME, LocalTime.class),
    /**
     * DATETIME[(fsp)]
     * A date and time combination. The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
     * MySQL displays DATETIME values in 'YYYY-MM-DD HH:MM:SS[.fraction]' format, but permits assignment of values to
     * DATETIME columns using either strings or numbers.
     * An optional fsp value in the range from 0 to 6 may be given to specify fractional seconds precision. A value
     * of 0 signifies that there is no fractional part. If omitted, the default precision is 0.
     * <p>
     * Protocol: TYPE_DATETIME = 12
     */
    DATETIME(Constants.TYPE_DATETIME, JDBCType.TIMESTAMP, LocalDateTime.class),
    /**
     * YEAR[(4)]
     * A year in four-digit format. MySQL displays YEAR values in YYYY format, but permits assignment of
     * values to YEAR columns using either strings or numbers. Values display as 1901 to 2155, and 0000.
     * Protocol: TYPE_YEAR = 13
     */
    YEAR(Constants.TYPE_YEAR, JDBCType.DATE, Year.class),
    /**
     * [NATIONAL] VARCHAR(M) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A variable-length string. M represents the maximum column length in characters. The range of M is 0 to 65,535.
     * The effective maximum length of a VARCHAR is subject to the maximum row size (65,535 bytes, which is shared among
     * all columns) and the character set used. For example, utf8 characters can require up to three bytes per character,
     * so a VARCHAR column that uses the utf8 character set can be declared to be a maximum of 21,844 characters.
     * <p>
     * MySQL stores VARCHAR values as a 1-byte or 2-byte length prefix plus data. The length prefix indicates the number
     * of bytes in the value. A VARCHAR column uses one length byte if values require no more than 255 bytes, two length
     * bytes if values may require more than 255 bytes.
     * <p>
     * Note
     * MySQL 5.7 follows the standard SQL specification, and does not remove trailing spaces from VARCHAR values.
     * <p>
     * VARCHAR is shorthand for CHARACTER VARYING. NATIONAL VARCHAR is the standard SQL way to define that a VARCHAR
     * column should use some predefined character set. MySQL 4.1 and up uses utf8 as this predefined character set.
     * NVARCHAR is shorthand for NATIONAL VARCHAR.
     * <p>
     * Protocol: TYPE_VARCHAR = 15
     * Protocol: TYPE_VAR_STRING = 253
     */
    VARCHAR(Constants.TYPE_VARCHAR, JDBCType.VARCHAR, String.class),
    /**
     * VARBINARY(M)
     * The VARBINARY type is similar to the VARCHAR type, but stores binary byte strings rather than nonbinary
     * character strings. M represents the maximum column length in bytes.
     * <p>
     * Protocol: TYPE_VARCHAR = 15
     * Protocol: TYPE_VAR_STRING = 253
     */
    VARBINARY(Constants.TYPE_VAR_STRING, JDBCType.VARBINARY, byte[].class), // TODO check that it's correct
    /**
     * BIT[(M)]
     * A bit-field type. M indicates the number of bits per value, from 1 to 64. The default is 1 if M is omitted.
     * Protocol: TYPE_BIT = 16
     */
    BIT(Constants.TYPE_BIT, JDBCType.BIT, Long.class),
    /**
     * The size of JSON documents stored in JSON columns is limited to the value of the max_allowed_packet system variable (max value 1073741824).
     * (While the server manipulates a JSON value internally in memory, it can be larger; the limit applies when the server stores it.)
     * <p>
     * Protocol: TYPE_BIT = 245
     */
    JSON(Constants.TYPE_JSON, JDBCType.LONGVARCHAR, String.class),
    /**
     * ENUM('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * An enumeration. A string object that can have only one value, chosen from the list of values 'value1',
     * 'value2', ..., NULL or the special '' error value. ENUM values are represented internally as integers.
     * An ENUM column can have a maximum of 65,535 distinct elements. (The practical limit is less than 3000.)
     * A table can have no more than 255 unique element list definitions among its ENUM and SET columns considered as a group
     * <p>
     * Protocol: TYPE_ENUM = 247
     */
    ENUM(Constants.TYPE_ENUM, JDBCType.CHAR, String.class),
    /**
     * SET('value1','value2',...) [CHARACTER SET charset_name] [COLLATE collation_name]
     * A set. A string object that can have zero or more values, each of which must be chosen from the list
     * of values 'value1', 'value2', ... SET values are represented internally as integers.
     * A SET column can have a maximum of 64 distinct members. A table can have no more than 255 unique
     * element list definitions among its ENUM and SET columns considered as a group
     * <p>
     * Protocol: TYPE_SET = 248
     */
    SET(Constants.TYPE_SET, JDBCType.CHAR, String.class),
    /**
     * TINYBLOB
     * A BLOB column with a maximum length of 255 (28 - 1) bytes. Each TINYBLOB value is stored using a
     * 1-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol:TYPE_TINY_BLOB = 249
     */
    TINYBLOB(Constants.TYPE_TINY_BLOB, JDBCType.VARBINARY, byte[].class),// TODO check that it's correct
    /**
     * TINYTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 255 (28 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TINYTEXT value is stored using
     * a 1-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol:TYPE_TINY_BLOB = 249
     */
    TINYTEXT(Constants.TYPE_TINY_BLOB, JDBCType.VARCHAR, String.class),
    /**
     * MEDIUMBLOB
     * A BLOB column with a maximum length of 16,777,215 (224 - 1) bytes. Each MEDIUMBLOB value is stored
     * using a 3-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol: TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMBLOB(Constants.TYPE_MEDIUM_BLOB, JDBCType.LONGVARBINARY, byte[].class),// TODO check that it's correct
    /**
     * MEDIUMTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 16,777,215 (224 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each MEDIUMTEXT value is stored using a 3-byte
     * length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol: TYPE_MEDIUM_BLOB = 250
     */
    MEDIUMTEXT(Constants.TYPE_MEDIUM_BLOB, JDBCType.LONGVARCHAR, String.class),
    /**
     * LONGBLOB
     * A BLOB column with a maximum length of 4,294,967,295 or 4GB (232 - 1) bytes. The effective maximum length
     * of LONGBLOB columns depends on the configured maximum packet size in the client/server protocol and available
     * memory. Each LONGBLOB value is stored using a 4-byte length prefix that indicates the number of bytes in the value.
     * <p>
     * Protocol: TYPE_LONG_BLOB = 251
     */
    LONGBLOB(Constants.TYPE_LONG_BLOB, JDBCType.LONGVARBINARY, InputStream.class),
    /**
     * LONGTEXT [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 4,294,967,295 or 4GB (232 - 1) characters. The effective
     * maximum length is less if the value contains multibyte characters. The effective maximum length
     * of LONGTEXT columns also depends on the configured maximum packet size in the client/server protocol
     * and available memory. Each LONGTEXT value is stored using a 4-byte length prefix that indicates
     * the number of bytes in the value.
     * <p>
     * Protocol: TYPE_LONG_BLOB = 251
     */
    LONGTEXT(Constants.TYPE_LONG_BLOB, JDBCType.LONGVARCHAR, Reader.class),
    /**
     * BLOB[(M)]
     * A BLOB column with a maximum length of 65,535 (216 - 1) bytes. Each BLOB value is stored using
     * a 2-byte length prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest BLOB type large enough to hold values M bytes long.
     * <p>
     * Protocol: TYPE_BLOB = 252
     */
    BLOB(Constants.TYPE_BLOB, JDBCType.LONGVARBINARY, byte[].class),// TODO check that it's correct
    /**
     * TEXT[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A TEXT column with a maximum length of 65,535 (216 - 1) characters. The effective maximum length
     * is less if the value contains multibyte characters. Each TEXT value is stored using a 2-byte length
     * prefix that indicates the number of bytes in the value.
     * An optional length M can be given for this type. If this is done, MySQL creates the column as
     * the smallest TEXT type large enough to hold values M characters long.
     * <p>
     * Protocol: TYPE_BLOB = 252
     */
    TEXT(Constants.TYPE_BLOB, JDBCType.LONGVARCHAR, String.class),
    /**
     * [NATIONAL] CHAR[(M)] [CHARACTER SET charset_name] [COLLATE collation_name]
     * A fixed-length string that is always right-padded with spaces to the specified length when stored.
     * M represents the column length in characters. The range of M is 0 to 255. If M is omitted, the length is 1.
     * Note
     * Trailing spaces are removed when CHAR values are retrieved unless the PAD_CHAR_TO_FULL_LENGTH SQL mode is enabled.
     * CHAR is shorthand for CHARACTER. NATIONAL CHAR (or its equivalent short form, NCHAR) is the standard SQL way
     * to define that a CHAR column should use some predefined character set. MySQL 4.1 and up uses utf8
     * as this predefined character set.
     * <p>
     * MySQL permits you to create a column of type CHAR(0). This is useful primarily when you have to be compliant
     * with old applications that depend on the existence of a column but that do not actually use its value.
     * CHAR(0) is also quite nice when you need a column that can take only two values: A column that is defined
     * as CHAR(0) NULL occupies only one bit and can take only the values NULL and '' (the empty string).
     * <p>
     * Protocol: TYPE_STRING = 254
     */
    CHAR(Constants.TYPE_STRING, JDBCType.CHAR, String.class),
    /**
     * BINARY(M)
     * The BINARY type is similar to the CHAR type, but stores binary byte strings rather than nonbinary character strings.
     * M represents the column length in bytes.
     * <p>
     * The CHAR BYTE data type is an alias for the BINARY data type.
     * <p>
     * Protocol: no concrete type on the wire TODO: really?
     */
    BINARY(Constants.TYPE_STRING, JDBCType.BINARY, byte[].class),// TODO check that it's correct
    /**
     * Top class for Spatial Data Types
     * <p>
     * Protocol: TYPE_GEOMETRY = 255
     */
    GEOMETRY(Constants.TYPE_GEOMETRY, JDBCType.OTHER, Geometry.class), // TODO check precision, it isn't well documented, only mentioned that WKB format is represented by BLOB
    /**
     * Fall-back type for those MySQL data types which c/J can't recognize.
     * Handled the same as BLOB.
     * <p>
     * Has no protocol ID.
     */
    UNKNOWN(-1, JDBCType.OTHER, Object.class);

    private static final Map<Integer, BiFunction<MySQLColumnMeta, Properties, MySQLType>> CONVERT_FUNCTION_MAP = createConverterMap();

    private static final List<MySQLType> VALUE_LIST = Collections.unmodifiableList(Arrays.asList(values()));

    private final JDBCType jdbcType;

    private final Class<?> javaType;

    public final int typeFlag;

    public final boolean unsigned;

    public final int parameterType;

    MySQLType(int typeFlag, JDBCType jdbcType, Class<?> javaType) {
        this(typeFlag, false, jdbcType, javaType);
    }

    MySQLType(int typeFlag, boolean unsigned, JDBCType jdbcType, Class<?> javaType) {
        this.typeFlag = typeFlag;
        this.unsigned = unsigned;
        this.jdbcType = jdbcType;
        this.javaType = javaType;

        this.parameterType = unsigned ? (128 | typeFlag) : typeFlag;
    }

    @Override
    public JDBCType jdbcType() {
        return this.jdbcType;
    }

    @Override
    public Class<?> javaType() {
        return this.javaType;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String getVendor() {
        return "io.jdbd.mysql";
    }

    @Override
    public boolean isUnsigned() {
        return this.unsigned;
    }


    @Override
    public Integer getVendorTypeNumber() {
        return this.typeFlag;
    }


    @Override
    public List<? extends SQLType> getFamily() {
        return VALUE_LIST;
    }


    public static MySQLType from(MySQLColumnMeta columnMeta, Properties properties) {
        BiFunction<MySQLColumnMeta, Properties, MySQLType> function;
        function = CONVERT_FUNCTION_MAP.get(columnMeta.typeFlag);
        return function == null ? UNKNOWN : function.apply(columnMeta, properties);
    }


    private static MySQLType fromDecimal(MySQLColumnMeta columnMeta, Properties properties) {
        return isUnsigned(columnMeta)
                ? DECIMAL_UNSIGNED
                : DECIMAL;
    }


    private static MySQLType fromTiny(MySQLColumnMeta columnMeta, Properties properties) {
        final boolean isUnsigned = isUnsigned(columnMeta);
        // Adjust for pseudo-boolean
        MySQLType mySQLType;
        if (!isUnsigned && columnMeta.length == 1
                && properties.getRequiredProperty(PropertyKey.tinyInt1isBit, Boolean.class)) {
            mySQLType = properties.getRequiredProperty(PropertyKey.transformedBitIsBoolean, Boolean.class)
                    ? BOOLEAN
                    : BIT;
        } else {
            mySQLType = isUnsigned ? MySQLType.TINYINT_UNSIGNED : MySQLType.TINYINT;
        }
        return mySQLType;
    }

    private static MySQLType fromBool(MySQLColumnMeta columnMeta, Properties properties) {
        return BOOLEAN;
    }

    private static MySQLType fromShort(MySQLColumnMeta columnMeta, Properties properties) {
        return isUnsigned(columnMeta)
                ? SMALLINT_UNSIGNED
                : SMALLINT;
    }

    private static MySQLType fromLong(MySQLColumnMeta columnMeta, Properties properties) {
        return isUnsigned(columnMeta)
                ? INT_UNSIGNED
                : INT;
    }

    private static MySQLType fromFloat(MySQLColumnMeta columnMeta, Properties properties) {
        return isUnsigned(columnMeta)
                ? FLOAT_UNSIGNED
                : FLOAT;
    }

    private static MySQLType fromDouble(MySQLColumnMeta columnMeta, Properties properties) {
        return isUnsigned(columnMeta)
                ? DOUBLE_UNSIGNED
                : DOUBLE;
    }

    private static MySQLType fromNull(MySQLColumnMeta columnMeta, Properties properties) {
        return MySQLType.NULL;
    }

    private static MySQLType fromTimestamp(MySQLColumnMeta columnMeta, Properties properties) {
        return MySQLType.TIMESTAMP;
    }

    private static MySQLType fromLongLong(MySQLColumnMeta columnMeta, Properties properties) {
        return isUnsigned(columnMeta)
                ? BIGINT_UNSIGNED
                : BIGINT;
    }

    private static MySQLType fromInt24(MySQLColumnMeta columnMeta, Properties properties) {
        return isUnsigned(columnMeta)
                ? MEDIUMINT_UNSIGNED
                : MEDIUMINT;
    }

    private static MySQLType fromDate(MySQLColumnMeta columnMeta, Properties properties) {
        return MySQLType.DATE;
    }

    private static MySQLType fromTime(MySQLColumnMeta columnMeta, Properties properties) {
        return MySQLType.TIME;
    }

    private static MySQLType fromDatetime(MySQLColumnMeta columnMeta, Properties properties) {
        return MySQLType.DATETIME;
    }

    private static MySQLType fromYear(MySQLColumnMeta columnMeta, Properties properties) {
        return MySQLType.YEAR;
    }

    private static MySQLType fromVarcharOrVarString(MySQLColumnMeta columnMeta, Properties properties) {
        return (isOpaqueBinary(columnMeta) && !isFunctionsNeverReturnBlobs(columnMeta, properties))
                ? VARBINARY : VARCHAR;
    }

    private static MySQLType fromBit(MySQLColumnMeta columnMeta, Properties properties) {
        return BIT;
    }

    private static MySQLType fromJson(MySQLColumnMeta columnMeta, Properties properties) {
        return JSON;
    }

    private static MySQLType fromEnum(MySQLColumnMeta columnMeta, Properties properties) {
        return ENUM;
    }

    private static MySQLType fromSet(MySQLColumnMeta columnMeta, Properties properties) {
        return SET;
    }

    private static MySQLType fromTinyBlob(MySQLColumnMeta columnMeta, Properties properties) {
        return isBlobTypeReturnText(columnMeta, properties) ? TINYTEXT : TINYBLOB;
    }

    private static MySQLType fromMediumBlob(MySQLColumnMeta columnMeta, Properties properties) {
        return isBlobTypeReturnText(columnMeta, properties) ? MEDIUMTEXT : MEDIUMBLOB;
    }

    private static MySQLType fromLongBlob(MySQLColumnMeta columnMeta, Properties properties) {
        return isBlobTypeReturnText(columnMeta, properties) ? LONGTEXT : LONGBLOB;
    }

    private static MySQLType fromBlob(MySQLColumnMeta columnMeta, Properties properties) {
        // Sometimes MySQL uses this protocol-level type for all possible BLOB variants,
        // we can divine what the actual type is by the length reported

        int newMysqlTypeId;

        final long maxLength = columnMeta.length;
        // fixing initial type according to length
        if (maxLength <= 255L) {
            newMysqlTypeId = Constants.TYPE_TINY_BLOB;
        } else if (columnMeta.length <= (1 << 16) - 1) {
            return isBlobTypeReturnText(columnMeta, properties) ? TEXT : BLOB;
        } else if (maxLength <= (1 << 24) - 1) {
            newMysqlTypeId = Constants.TYPE_MEDIUM_BLOB;
        } else {
            newMysqlTypeId = Constants.TYPE_LONG_BLOB;
        }

        // call this method again with correct this.mysqlType set
        return MySQLObjects.requireNonNull(
                CONVERT_FUNCTION_MAP.get(newMysqlTypeId), "TypeFlag[%s] no function", newMysqlTypeId)
                .apply(columnMeta, properties);
    }

    private static MySQLType fromString(MySQLColumnMeta columnMeta, Properties properties) {
        return (isOpaqueBinary(columnMeta)
                && !properties.getRequiredProperty(PropertyKey.blobsAreStrings, Boolean.class))
                ? BINARY
                : CHAR;
    }

    private static MySQLType fromGeometry(MySQLColumnMeta columnMeta, Properties properties) {
        return GEOMETRY;
    }


    private static boolean isOpaqueBinary(MySQLColumnMeta columnMeta) {

        boolean isImplicitTemporaryTable = columnMeta.tableName != null
                && columnMeta.tableName.startsWith("#sql_"); //TODO check tableName or tableAlias

        boolean isBinaryString = (isBinary(columnMeta.definitionFlags)
                && columnMeta.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary
                && (columnMeta.typeFlag == Constants.TYPE_STRING
                || columnMeta.typeFlag == Constants.TYPE_VAR_STRING
                || columnMeta.typeFlag == Constants.TYPE_VARCHAR));

        return isBinaryString
                // queries resolved by temp tables also have this 'signature', check for that
                ? !isImplicitTemporaryTable : columnMeta.collationIndex == CharsetMapping.MYSQL_COLLATION_INDEX_binary;

    }

    private static boolean isUnsigned(MySQLColumnMeta columnMeta) {
        return ((columnMeta.definitionFlags & MySQLColumnMeta.UNSIGNED_FLAG) != 0);
    }

    private static boolean isBinary(final int definitionFlags) {
        return (definitionFlags & MySQLColumnMeta.BINARY_FLAG) != 0;
    }

    private static boolean isFunctionsNeverReturnBlobs(MySQLColumnMeta columnMeta, Properties properties) {
        return StringUtils.isEmpty(columnMeta.tableName)
                && properties.getRequiredProperty(PropertyKey.functionsNeverReturnBlobs, Boolean.class);
    }

    private static boolean isBlobTypeReturnText(MySQLColumnMeta columnMeta, Properties properties) {
        return !isBinary(columnMeta.definitionFlags)
                || columnMeta.collationIndex != CharsetMapping.MYSQL_COLLATION_INDEX_binary
                || properties.getRequiredProperty(PropertyKey.blobsAreStrings, Boolean.class)
                || isFunctionsNeverReturnBlobs(columnMeta, properties);
    }

    private static Map<Integer, BiFunction<MySQLColumnMeta, Properties, MySQLType>> createConverterMap() {
        Map<Integer, BiFunction<MySQLColumnMeta, Properties, MySQLType>> map;
        map = new HashMap<>();

        map.put(Constants.TYPE_DECIMAL, MySQLType::fromDecimal);
        map.put(Constants.TYPE_TINY, MySQLType::fromTiny);
        map.put(Constants.TYPE_SHORT, MySQLType::fromShort);
        map.put(Constants.TYPE_LONG, MySQLType::fromLong);

        map.put(Constants.TYPE_FLOAT, MySQLType::fromFloat);
        map.put(Constants.TYPE_DOUBLE, MySQLType::fromDouble);
        map.put(Constants.TYPE_NULL, MySQLType::fromNull);
        map.put(Constants.TYPE_TIMESTAMP, MySQLType::fromTimestamp);

        map.put(Constants.TYPE_LONGLONG, MySQLType::fromLongLong);
        map.put(Constants.TYPE_INT24, MySQLType::fromInt24);
        map.put(Constants.TYPE_DATE, MySQLType::fromDate);
        map.put(Constants.TYPE_TIME, MySQLType::fromTime);

        map.put(Constants.TYPE_DATETIME, MySQLType::fromDatetime);
        map.put(Constants.TYPE_YEAR, MySQLType::fromYear);
        map.put(Constants.TYPE_VARCHAR, MySQLType::fromVarcharOrVarString);
        map.put(Constants.TYPE_BIT, MySQLType::fromBit);

        map.put(Constants.TYPE_JSON, MySQLType::fromJson);
        map.put(Constants.TYPE_NEWDECIMAL, MySQLType::fromDecimal);
        map.put(Constants.TYPE_ENUM, MySQLType::fromEnum);
        map.put(Constants.TYPE_SET, MySQLType::fromSet);

        map.put(Constants.TYPE_TINY_BLOB, MySQLType::fromTinyBlob);
        map.put(Constants.TYPE_MEDIUM_BLOB, MySQLType::fromMediumBlob);
        map.put(Constants.TYPE_LONG_BLOB, MySQLType::fromLongBlob);
        map.put(Constants.TYPE_BLOB, MySQLType::fromBlob);

        map.put(Constants.TYPE_VAR_STRING, MySQLType::fromVarcharOrVarString);
        map.put(Constants.TYPE_STRING, MySQLType::fromString);
        map.put(Constants.TYPE_BOOL, MySQLType::fromBool);
        map.put(Constants.TYPE_GEOMETRY, MySQLType::fromGeometry);


        return Collections.unmodifiableMap(map);
    }


}
