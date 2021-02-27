package io.jdbd;


import io.jdbd.lang.Nullable;
import reactor.util.annotation.NonNull;

import java.sql.SQLException;

public final class JdbdSQLException extends JdbdException {

    public JdbdSQLException(SQLException cause) {
        super(cause, cause.getMessage());
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

    public final int getVendorCode() {
        return getCause().getErrorCode();
    }

    @Nullable
    public final String getSQLState() {
        return getCause().getSQLState();
    }


}
