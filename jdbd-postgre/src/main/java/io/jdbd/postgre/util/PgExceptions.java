package io.jdbd.postgre.util;

import io.jdbd.JdbdSQLException;
import io.jdbd.postgre.protocol.client.ErrorMessage;
import io.jdbd.vendor.util.JdbdExceptions;

import java.sql.SQLException;


public abstract class PgExceptions extends JdbdExceptions {


    public static JdbdSQLException createErrorException(ErrorMessage error) {
        SQLException e = new SQLException(error.getMessage(), error.getSQLState());
        return new JdbdSQLException(e);
    }


}
