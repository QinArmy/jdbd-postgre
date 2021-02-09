package io.jdbd.mysql.protocol;


import java.time.Duration;

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

    long DURATION_MAX_SECONDS = Duration.ofHours(838)
            .plusMinutes(59)
            .plusSeconds(59)
            .plusMillis(999)
            .getSeconds();


    //below  Protocol field type numbers, see https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html#a69e798807026a0f7e12b1d6c72374854
    //
    int TYPE_DECIMAL = 0;
    int TYPE_TINY = 1;
    int TYPE_SHORT = 2;
    int TYPE_LONG = 3;
    int TYPE_FLOAT = 4;
    int TYPE_DOUBLE = 5;
    int TYPE_NULL = 6;
    int TYPE_TIMESTAMP = 7;
    int TYPE_LONGLONG = 8;
    int TYPE_INT24 = 9;
    int TYPE_DATE = 10;
    int TYPE_TIME = 11;
    int TYPE_DATETIME = 12;
    int TYPE_YEAR = 13;
    int TYPE_VARCHAR = 15;
    int TYPE_BIT = 16;
    int TYPE_BOOL = 244;
    int TYPE_JSON = 245;
    int TYPE_ENUM = 247;
    int TYPE_SET = 248;
    int TYPE_TINY_BLOB = 249;
    int TYPE_MEDIUM_BLOB = 250;
    int TYPE_LONG_BLOB = 251;
    int TYPE_BLOB = 252;
    int TYPE_VAR_STRING = 253;
    int TYPE_STRING = 254;
    int TYPE_GEOMETRY = 255;
    int TYPE_NEWDECIMAL = 246;
}
