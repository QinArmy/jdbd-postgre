package io.jdbd.postgre.util;

import io.jdbd.postgre.PgConstant;
import io.jdbd.vendor.util.JdbdStrings;

public abstract class PgStrings extends JdbdStrings {

    protected PgStrings() {
        throw new UnsupportedOperationException();
    }


    public static boolean parseBoolean(final String textValue) {
        final boolean value;
        if (textValue.equalsIgnoreCase("t")
                || textValue.equalsIgnoreCase(PgConstant.TRUE)) {
            value = true;
        } else if (textValue.equalsIgnoreCase("f")
                || textValue.equalsIgnoreCase(PgConstant.FALSE)) {
            value = false;
        } else {
            throw new IllegalArgumentException(String.format("textValue[%s] isn't boolean.", textValue));
        }
        return value;
    }


}
