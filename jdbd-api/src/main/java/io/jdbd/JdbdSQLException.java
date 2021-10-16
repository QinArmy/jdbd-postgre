package io.jdbd;


import io.jdbd.lang.NonNull;
import io.jdbd.lang.Nullable;

import java.sql.SQLException;

public final class JdbdSQLException extends JdbdException {

    public JdbdSQLException(SQLException cause) {
        super(cause, cause.getMessage());
    }

    public JdbdSQLException(String message, SQLException cause) {
        super(message, cause);
    }

    @Deprecated
    public JdbdSQLException(SQLException cause, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause);
    }

    @Deprecated
    public JdbdSQLException(SQLException cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause, enableSuppression, writableStackTrace);
    }

    @NonNull
    @Override
    public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
    }

    public int getVendorCode() {
        return getCause().getErrorCode();
    }

    @Nullable
    public String getSQLState() {
        return getCause().getSQLState();
    }


}
