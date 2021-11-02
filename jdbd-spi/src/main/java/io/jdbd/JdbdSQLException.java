package io.jdbd;


import io.jdbd.lang.Nullable;

import java.sql.SQLException;

public class JdbdSQLException extends JdbdException {

    public static JdbdSQLException create(final SQLException cause) {
        final String sqlState = cause.getSQLState();
        final JdbdSQLException e;
        if (sqlState == null) {
            e = new JdbdSQLException(cause);
        } else {
            Integer errorCode = JdbdXaException.getXaErrorCode(sqlState);
            if (errorCode == null) {
                e = new JdbdSQLException(cause);
            } else {
                e = new JdbdXaException(cause, errorCode);
            }
        }
        return e;
    }

    private final String sqlState;

    private final int vendorCode;

    JdbdSQLException(String reason, @Nullable String sqlState, int vendorCode) {
        super(reason);
        this.sqlState = sqlState;
        this.vendorCode = vendorCode;
    }


    public JdbdSQLException(SQLException cause) {
        super(cause, cause.getMessage());
        this.sqlState = cause.getSQLState();
        this.vendorCode = cause.getErrorCode();
    }

    @Deprecated
    public JdbdSQLException(String message, SQLException cause) {
        super(message, cause);
        this.sqlState = cause.getSQLState();
        this.vendorCode = cause.getErrorCode();
    }

    @Deprecated
    public JdbdSQLException(SQLException cause, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause);
        this.sqlState = cause.getSQLState();
        this.vendorCode = cause.getErrorCode();
    }

    @Deprecated
    public JdbdSQLException(SQLException cause, boolean enableSuppression
            , boolean writableStackTrace, String messageFormat, Object... args) {
        super(createMessage(messageFormat, args), cause, enableSuppression, writableStackTrace);
        this.sqlState = cause.getSQLState();
        this.vendorCode = cause.getErrorCode();
    }

    public final int getVendorCode() {
        return this.vendorCode;
    }

    @Nullable
    public final String getSQLState() {
        return this.sqlState;
    }


}
