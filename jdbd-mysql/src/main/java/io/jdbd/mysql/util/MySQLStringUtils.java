package io.jdbd.mysql.util;

import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;

public abstract class MySQLStringUtils extends org.qinarmy.util.StringUtils {

    protected MySQLStringUtils() {
        throw new UnsupportedOperationException();
    }


    public static byte[] getBytesNullTerminated(String text, Charset charset) {
        byte[] textBytes = text.getBytes(charset);
        byte[] bytes = new byte[textBytes.length + 1];

        System.arraycopy(textBytes, 0, bytes, 0, textBytes.length);
        bytes[textBytes.length] = 0;
        return bytes;
    }

    @Nullable
    public static Boolean tryConvertToBoolean(String text) {
        Boolean value;
        if (text.equalsIgnoreCase("true")
                || text.equalsIgnoreCase("Y")
                || text.equalsIgnoreCase("T")) {
            value = Boolean.TRUE;

        } else if (text.equalsIgnoreCase("false")
                || text.equalsIgnoreCase("N")
                || text.equalsIgnoreCase("F")) {
            value = Boolean.FALSE;
        } else {
            value = null;
        }
        return value;

    }

}
