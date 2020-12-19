package io.jdbd.mysql.util;

import io.jdbd.JdbdSQLException;
import io.jdbd.PreparedStatement;
import io.jdbd.Statement;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.MySQLFatalIoException;
import org.qinarmy.util.security.ExceptionUtils;

import java.sql.SQLException;

public abstract class MySQLExceptionUtils extends ExceptionUtils {

    protected MySQLExceptionUtils() {
        throw new UnsupportedOperationException();
    }

    public static JdbdSQLException wrapSQLExceptionIfNeed(Throwable t) {
        JdbdSQLException e;
        if (t instanceof JdbdSQLException) {
            e = (JdbdSQLException) t;
        } else if (t instanceof SQLException) {
            e = new JdbdSQLException((SQLException) t);
        } else {
            e = new JdbdSQLException(new SQLException(t));
        }
        return e;
    }

    public static JdbdMySQLException wrapJdbdExceptionIfNeed(Throwable t) {
        JdbdMySQLException e;
        if (t instanceof JdbdMySQLException) {
            e = (JdbdMySQLException) t;
        } else {
            e = new JdbdMySQLException(t, t.getMessage());
        }
        return e;
    }

    public static JdbdSQLException createErrorPacketException(ErrorPacket error) {
        return new JdbdSQLException(createSQLException(error));
    }

    public static SQLException createSQLException(ErrorPacket error) {
        return new SQLException(error.getErrorMessage(), error.getSqlState());
    }

    public static JdbdSQLException createNonResultSetCommandException() {
        String message = "SQL isn't query command,please use " + Statement.class.getName() +
                ".executeQuery(String,BiFunction<ResultRow,ResultRowMeta,T>, Consumer<ResultStates>) method";
        return new JdbdSQLException(new SQLException(message));
    }

    public static JdbdSQLException createNonCommandUpdateException() {
        String message = "SQL isn't dml command,please use %s.executeUpdate() or %s.executeUpdate(String ) method";
        message = String.format(message, PreparedStatement.class.getName(), Statement.class.getName());
        return new JdbdSQLException(new SQLException(message));
    }

    public static JdbdSQLException createFatalIoException(@Nullable Throwable e, String format
            , @Nullable Object... args) {
        String message;
        if (args == null || args.length == 0) {
            message = format;
        } else {
            message = String.format(format, args);
        }
        return new JdbdSQLException(new MySQLFatalIoException(message, e));
    }

    public static JdbdSQLException createFatalIoException(String format, @Nullable Object... args) {
        return createFatalIoException(null, format, args);
    }


    public static boolean containFatalIoException(final Throwable e) {
        return containException(e, MySQLFatalIoException.class);
    }


}
