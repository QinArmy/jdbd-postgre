package io.jdbd.mysql.util;

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

}
