package io.jdbd.vendor.util;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.vendor.JdbdUnknownException;

import java.sql.SQLException;

public abstract class JdbdExceptions {

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
            je = new JdbdUnknownException(e, "Unknown error,%s", e.getMessage());
        }
        return je;
    }

}
