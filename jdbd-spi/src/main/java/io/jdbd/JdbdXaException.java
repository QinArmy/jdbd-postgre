package io.jdbd;


import io.jdbd.lang.Nullable;

import java.sql.SQLException;


public final class JdbdXaException extends JdbdSQLException {

    public static final int XA_RBBASE = 100;
    public static final int XA_RBROLLBACK = 100;
    public static final int XA_RBCOMMFAIL = 101;
    public static final int XA_RBDEADLOCK = 102;
    public static final int XA_RBINTEGRITY = 103;
    public static final int XA_RBOTHER = 104;
    public static final int XA_RBPROTO = 105;
    public static final int XA_RBTIMEOUT = 106;
    public static final int XA_RBTRANSIENT = 107;
    public static final int XA_RBEND = 107;
    public static final int XA_NOMIGRATE = 9;
    public static final int XA_HEURHAZ = 8;
    public static final int XA_HEURCOM = 7;
    public static final int XA_HEURRB = 6;
    public static final int XA_HEURMIX = 5;
    public static final int XA_RETRY = 4;
    public static final int XA_RDONLY = 3;
    public static final int XAER_ASYNC = -2;
    public static final int XAER_RMERR = -3;
    public static final int XAER_NOTA = -4;
    public static final int XAER_INVAL = -5;
    public static final int XAER_PROTO = -6;
    public static final int XAER_RMFAIL = -7;
    public static final int XAER_DUPID = -8;
    public static final int XAER_OUTSIDE = -9;

    private final int xaErrorCode;

    public JdbdXaException(String reason, @Nullable String sqlState, int XaErrorCode) {
        super(reason, sqlState, 0);
        this.xaErrorCode = XaErrorCode;
    }

    JdbdXaException(SQLException cause, int xaErrorCode) {
        super(cause);
        this.xaErrorCode = xaErrorCode;
    }


    public int getXaErrorCode() {
        return this.xaErrorCode;
    }


    @Nullable
    public static Integer getXaErrorCode(final String sqlState) {
        final Integer errorCode;
        switch (sqlState) {
            case "XAE04": {
                errorCode = JdbdXaException.XAER_NOTA;
            }
            break;
            case "XAE05": {
                errorCode = JdbdXaException.XAER_INVAL;
            }
            break;
            case "XAE07": {
                errorCode = JdbdXaException.XAER_RMFAIL;
            }
            break;
            case "XAE09": {
                errorCode = JdbdXaException.XAER_OUTSIDE;
            }
            break;
            case "XAE03": {
                errorCode = JdbdXaException.XAER_RMERR;
            }
            break;
            case "XA100": {
                errorCode = JdbdXaException.XA_RBROLLBACK;
            }
            break;
            case "XAE08": {
                errorCode = JdbdXaException.XAER_DUPID;
            }
            break;
            case "XA106": {
                errorCode = JdbdXaException.XA_RBTIMEOUT;
            }
            break;
            case "XA102": {
                errorCode = JdbdXaException.XA_RBDEADLOCK;
            }
            break;
            default: {
                errorCode = null;
            }
        }
        return errorCode;
    }


}
