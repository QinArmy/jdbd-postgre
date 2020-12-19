package io.jdbd.mysql.protocol;


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


}
