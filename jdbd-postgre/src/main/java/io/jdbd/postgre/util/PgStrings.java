package io.jdbd.postgre.util;

import io.jdbd.vendor.util.JdbdStrings;

public abstract class PgStrings extends JdbdStrings {

    protected PgStrings() {
        throw new UnsupportedOperationException();
    }


    public static boolean isSafePgString(String text) {
        final char[] charArray = text.toCharArray();
        boolean match = true;
        for (char c : charArray) {
            if (c == '\'' || c == '\\') {
                match = false;
                break;
            }
        }
        return match;
    }

}
