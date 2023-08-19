package io.jdbd.postgre.util;

import io.jdbd.JdbdException;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdExceptions;


public abstract class PgExceptions extends JdbdExceptions {

    private PgExceptions() {
    }

    public static JdbdException sqlHaveNoText() {
        return new JdbdException("sql must have text.");
    }

    public static JdbdException createObjectTooLargeError() {
        return new JdbdException("SQL too large to send over the protocol");
    }

    public static JdbdException createBindCountNotMatchError(int stmtIndex, int paramCount, int valueSize) {
        String m = String.format("Statement[%s] parameter placeholder count[%s] and bind value count[%s] not match."
                , stmtIndex, paramCount, valueSize);
        return new JdbdException(m);
    }

    public static JdbdException createBindIndexNotMatchError(int stmtIndex, int placeholderIndex, ParamValue bindValue) {
        String m = String.format("Statement[%s] parameter placeholder number[%s] and bind index[%s] not match."
                , stmtIndex, placeholderIndex, bindValue.getIndex());
        return new JdbdException(m);
    }

    public static JdbdException createNotSupportBindTypeError(int stmtIndex, ParamValue bindValue) {
        String m = String.format("Statement[%s] parameter[%s] java type[%s] couldn't bind to postgre type[%s]"
                , stmtIndex, bindValue.getIndex()
                , bindValue.getNonNull().getClass().getName(), bindValue.getType());
        return new JdbdException(m);
    }


}
