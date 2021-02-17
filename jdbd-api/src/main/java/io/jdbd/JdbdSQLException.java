package io.jdbd;


import reactor.util.annotation.NonNull;

import java.sql.SQLException;

public final class JdbdSQLException extends JdbdException {

    public JdbdSQLException(SQLException cause) {
        super(cause);
    }

    public JdbdSQLException(String message, SQLException cause) {
        super(message, cause);
    }

    public JdbdSQLException(String message, SQLException cause
            , boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public JdbdSQLException(String messageFormat, Object... args) {
        super(createMessage(messageFormat, args));
    }

    public JdbdSQLException(SQLException cause, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause);
    }

    public JdbdSQLException(SQLException cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause, enableSuppression, writableStackTrace);
    }

    @NonNull
    @Override
    public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
    }
}
