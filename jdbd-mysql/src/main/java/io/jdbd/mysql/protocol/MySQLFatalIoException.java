package io.jdbd.mysql.protocol;

import java.sql.SQLException;

public final class MySQLFatalIoException extends SQLException {

    public MySQLFatalIoException(String reason) {
        super(reason);
    }

    public MySQLFatalIoException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public MySQLFatalIoException(String reason, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
    }
}
