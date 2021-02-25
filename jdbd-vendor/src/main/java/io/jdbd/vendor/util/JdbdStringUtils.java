package io.jdbd.vendor.util;

import org.qinarmy.util.StringUtils;

public abstract class JdbdStringUtils extends StringUtils {

    protected JdbdStringUtils() {
        throw new UnsupportedOperationException();
    }


    public static int skipBlockComment(final String sql, final String startMarker, final String endMarker) {
        return skipBlockComment(sql, 0, startMarker, endMarker);
    }

    /**
     * @return [0, length] ,when return length of sql,sql start with {@code startMarker} but not end with {@code endMarker}.
     */
    public static int skipBlockComment(final String sql, final int offset, final String startMarker
            , final String endMarker) {
        final int length = sql.length();
        if (offset < 0 || offset >= length) {
            throw new IllegalArgumentException(String.format("sql length[%s] but offset[%s].", length, offset));
        }
        char ch;
        for (int i = offset; i < length; i++) {
            ch = sql.charAt(i);
            if (Character.isWhitespace(ch)) {
                continue;
            }
            if (sql.startsWith(startMarker, i)) {
                i += startMarker.length();
            }
        }
        return 0;
    }

}
