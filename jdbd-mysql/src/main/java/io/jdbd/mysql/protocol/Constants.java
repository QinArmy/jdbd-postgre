package io.jdbd.mysql.protocol;


import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Represents various constants used in the driver.
 * <p>
 * see {@code com.mysql.cj.Constants}
 * </p>
 */
public interface Constants {

    /**
     * Avoids allocation of empty byte[] when representing 0-length strings.
     */
    byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * I18N'd representation of the abbreviation for "ms"
     */
    String MILLIS_I18N = "ms";

    byte[] SLASH_STAR_SPACE_AS_BYTES = new byte[]{(byte) '/', (byte) '*', (byte) ' '};

    byte[] SPACE_STAR_SLASH_SPACE_AS_BYTES = new byte[]{(byte) ' ', (byte) '*', (byte) '/', (byte) ' '};

    String JVM_VENDOR = System.getProperty("java.vendor");
    String JVM_VERSION = System.getProperty("java.version");

//    String OS_NAME = System.getProperty(PropertyDefinitions.SYSP_os_name);
//    String OS_ARCH = System.getProperty(PropertyDefinitions.SYSP_os_arch);
//    String OS_VERSION = System.getProperty(PropertyDefinitions.SYSP_os_version);
//    String PLATFORM_ENCODING = System.getProperty(PropertyDefinitions.SYSP_file_encoding);

    String CJ_NAME = "MySQL Connector/J";
    String CJ_FULL_NAME = "mysql-connector-java-8.0.21";
    String CJ_REVISION = "33f65445a1bcc544eb0120491926484da168f199";
    String CJ_VERSION = "8.0.21";
    String CJ_MAJOR_VERSION = "8";
    String CJ_MINOR_VERSION = "0";
    String CJ_LICENSE = "GPL";

    BigInteger BIG_INTEGER_ZERO = BigInteger.valueOf(0);
    BigInteger BIG_INTEGER_ONE = BigInteger.valueOf(1);
    BigInteger BIG_INTEGER_NEGATIVE_ONE = BigInteger.valueOf(-1);
    BigInteger BIG_INTEGER_MIN_BYTE_VALUE = BigInteger.valueOf(Byte.MIN_VALUE);
    BigInteger BIG_INTEGER_MAX_BYTE_VALUE = BigInteger.valueOf(Byte.MAX_VALUE);
    BigInteger BIG_INTEGER_MIN_SHORT_VALUE = BigInteger.valueOf(Short.MIN_VALUE);
    BigInteger BIG_INTEGER_MAX_SHORT_VALUE = BigInteger.valueOf(Short.MAX_VALUE);
    BigInteger BIG_INTEGER_MIN_INTEGER_VALUE = BigInteger.valueOf(Integer.MIN_VALUE);
    BigInteger BIG_INTEGER_MAX_INTEGER_VALUE = BigInteger.valueOf(Integer.MAX_VALUE);
    BigInteger BIG_INTEGER_MIN_LONG_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
    BigInteger BIG_INTEGER_MAX_LONG_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

    BigDecimal BIG_DECIMAL_ZERO = BigDecimal.valueOf(0);
    BigDecimal BIG_DECIMAL_ONE = BigDecimal.valueOf(1);
    BigDecimal BIG_DECIMAL_NEGATIVE_ONE = BigDecimal.valueOf(-1);
    BigDecimal BIG_DECIMAL_MIN_BYTE_VALUE = BigDecimal.valueOf(Byte.MIN_VALUE);
    BigDecimal BIG_DECIMAL_MAX_BYTE_VALUE = BigDecimal.valueOf(Byte.MAX_VALUE);
    BigDecimal BIG_DECIMAL_MIN_SHORT_VALUE = BigDecimal.valueOf(Short.MIN_VALUE);
    BigDecimal BIG_DECIMAL_MAX_SHORT_VALUE = BigDecimal.valueOf(Short.MAX_VALUE);
    BigDecimal BIG_DECIMAL_MIN_INTEGER_VALUE = BigDecimal.valueOf(Integer.MIN_VALUE);
    BigDecimal BIG_DECIMAL_MAX_INTEGER_VALUE = BigDecimal.valueOf(Integer.MAX_VALUE);
    BigDecimal BIG_DECIMAL_MIN_LONG_VALUE = BigDecimal.valueOf(Long.MIN_VALUE);
    BigDecimal BIG_DECIMAL_MAX_LONG_VALUE = BigDecimal.valueOf(Long.MAX_VALUE);
    BigDecimal BIG_DECIMAL_MAX_DOUBLE_VALUE = BigDecimal.valueOf(Double.MAX_VALUE);
    BigDecimal BIG_DECIMAL_MAX_NEGATIVE_DOUBLE_VALUE = BigDecimal.valueOf(-Double.MAX_VALUE);
    BigDecimal BIG_DECIMAL_MAX_FLOAT_VALUE = BigDecimal.valueOf(Float.MAX_VALUE);
    BigDecimal BIG_DECIMAL_MAX_NEGATIVE_FLOAT_VALUE = BigDecimal.valueOf(-Float.MAX_VALUE);

}
