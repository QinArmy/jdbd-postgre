package io.jdbd.vendor.util;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.vendor.JdbdUnknownException;
import org.qinarmy.util.ExceptionUtils;
import reactor.util.annotation.Nullable;

import java.sql.SQLException;

public abstract class JdbdExceptions extends ExceptionUtils {

    protected JdbdExceptions() {
        throw new UnsupportedOperationException();
    }


    public static JdbdException wrap(Throwable e) {
        JdbdException je;
        if (e instanceof JdbdException) {
            je = (JdbdException) e;
        } else if (e instanceof SQLException) {
            je = new JdbdSQLException((SQLException) e);
        } else {
            je = new JdbdUnknownException(String.format("Unknown error,%s", e.getMessage()), e);
        }
        return je;
    }

    public static JdbdException wrap(Throwable e, String format, @Nullable Object... args) {
        final String message;
        if (args == null || args.length == 0) {
            message = format;
        } else {
            message = String.format(format, args);
        }
        JdbdException je;
        if (e instanceof JdbdException) {
            je = (JdbdException) e;
        } else if (e instanceof SQLException) {
            je = new JdbdSQLException((SQLException) e, message);
        } else {
            je = new JdbdUnknownException(message, e);
        }
        return je;
    }


}
