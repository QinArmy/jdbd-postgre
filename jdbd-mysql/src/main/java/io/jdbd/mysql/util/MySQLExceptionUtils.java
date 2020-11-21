package io.jdbd.mysql.util;

import io.jdbd.PreparedStatement;
import io.jdbd.ReactiveSQLException;
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

    public static ReactiveSQLException wrapSQLExceptionIfNeed(Throwable t) {
        ReactiveSQLException e;
        if (t instanceof ReactiveSQLException) {
            e = (ReactiveSQLException) t;
        } else if (t instanceof SQLException) {
            e = new ReactiveSQLException((SQLException) t);
        } else {
            e = new ReactiveSQLException(new SQLException(t));
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

    public static ReactiveSQLException createErrorPacketException(ErrorPacket error) {
        return new ReactiveSQLException(new SQLException(error.getErrorMessage(), error.getSqlState()));
    }

    public static ReactiveSQLException createNonResultSetCommandException() {
        String message = "SQL isn't query command,please use " + Statement.class.getName() +
                ".executeQuery(String,BiFunction<ResultRow,ResultRowMeta,T>, Consumer<ResultStates>) method";
        return new ReactiveSQLException(new SQLException(message));
    }

    public static ReactiveSQLException createNonCommandUpdateException() {
        String message = "SQL isn't dml command,please use %s.executeUpdate() or %s.executeUpdate(String ) method";
        message = String.format(message, PreparedStatement.class.getName(), Statement.class.getName());
        return new ReactiveSQLException(new SQLException(message));
    }

    public static ReactiveSQLException createFatalIoException(@Nullable Throwable e, String format
            , @Nullable Object... args) {
        String message;
        if (args == null || args.length == 0) {
            message = format;
        } else {
            message = String.format(format, args);
        }
        return new ReactiveSQLException(new MySQLFatalIoException(message, e));
    }

    public static ReactiveSQLException createFatalIoException(String format, @Nullable Object... args) {
        return createFatalIoException(null, format, args);
    }

    public static boolean containFatalIoException(final Throwable e) {
        return containException(e, MySQLFatalIoException.class);
    }


}
