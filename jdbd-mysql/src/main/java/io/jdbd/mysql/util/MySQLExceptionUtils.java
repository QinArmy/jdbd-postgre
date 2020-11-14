package io.jdbd.mysql.util;

import io.jdbd.PreparedStatement;
import io.jdbd.ReactiveSQLException;
import io.jdbd.Statement;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.MySQLFatalIoException;
import reactor.util.annotation.Nullable;

import java.sql.SQLException;

public abstract class MySQLExceptionUtils {

    protected MySQLExceptionUtils() {
        throw new UnsupportedOperationException();
    }

    public static ReactiveSQLException wrapExceptionIfNeed(Throwable t) {
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

    public static ReactiveSQLException createErrorPacketException(ErrorPacket error) {
        return new ReactiveSQLException(new SQLException(error.getErrorMessage(), error.getSqlState()));
    }

    public static ReactiveSQLException createNonResultSetCommandException() {
        String message = "SQL isn't query command,please use " + Statement.class.getName() +
                ".executeQuery(String,BiFunction<ResultRow,ResultRowMeta,T>, Consumer<ResultStates>) method";
        return new ReactiveSQLException(new SQLException(message));
    }

    public static ReactiveSQLException createNonCommandException() {
        String message = "SQL isn't dml command,please use %s.executeUpdate() or %s.executeUpdate(String ) method";
        message = String.format(message, PreparedStatement.class.getName(), Statement.class.getName());
        return new ReactiveSQLException(new SQLException(message));
    }

    public static ReactiveSQLException createFatalIoException(String format, @Nullable Object... args) {
        String message;
        if (args == null || args.length == 0) {
            message = format;
        } else {
            message = String.format(format, args);
        }
        return new ReactiveSQLException(new MySQLFatalIoException(message));
    }

    public static boolean isContainFatalIoException(final Throwable e) {
        Throwable t = e;
        boolean match = false;
        while (t != null) {
            if (t instanceof MySQLFatalIoException) {
                match = true;
                break;
            }
            t = t.getCause();
        }
        return match;
    }


}