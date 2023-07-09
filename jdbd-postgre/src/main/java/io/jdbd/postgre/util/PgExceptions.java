package io.jdbd.postgre.util;

import io.jdbd.postgre.protocol.client.ErrorMessage;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.vendor.util.JdbdExceptions;

import java.sql.SQLException;


public abstract class PgExceptions extends JdbdExceptions {


    public static JdbdSQLException createErrorException(ErrorMessage error) {
        SQLException e = new SQLException(error.getMessage(), error.getSQLState());
        return new JdbdSQLException(e);
    }


    public static SQLException createObjectTooLargeError() {
        return new SQLException("SQL too large to send over the protocol");
    }

    public static SQLException createBindCountNotMatchError(int stmtIndex, int paramCount, int valueSize) {
        String m = String.format("Statement[%s] parameter placeholder count[%s] and bind value count[%s] not match."
                , stmtIndex, paramCount, valueSize);
        return new SQLException(m);
    }

    public static SQLException createBindIndexNotMatchError(int stmtIndex, int placeholderIndex, BindValue bindValue) {
        String m = String.format("Statement[%s] parameter placeholder number[%s] and bind index[%s] not match."
                , stmtIndex, placeholderIndex, bindValue.getIndex());
        return new SQLException(m);
    }

    public static SQLException createNotSupportBindTypeError(int stmtIndex, BindValue bindValue) {
        String m = String.format("Statement[%s] parameter[%s] java type[%s] couldn't bind to postgre type[%s]"
                , stmtIndex, bindValue.getIndex()
                , bindValue.getNonNull().getClass().getName(), bindValue.getType());
        return new SQLException(m);
    }


}
