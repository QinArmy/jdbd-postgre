package io.jdbd.vendor.util;

import org.qinarmy.util.NumberUtils;

public abstract class JdbdNumbers extends NumberUtils {

    protected JdbdNumbers() {
        throw new UnsupportedOperationException();
    }


    public static short mapToShort(Object nonNull) {
        short value;
        if (nonNull instanceof Number) {
            value = convertNumberToShort((Number) nonNull);
        } else if (nonNull instanceof String) {
            value = Short.parseShort((String) nonNull);
        } else {
            String m = String.format("Not support java type[%s] to short.", nonNull.getClass().getName());
            throw new IllegalArgumentException(m);
        }
        return value;
    }

}
