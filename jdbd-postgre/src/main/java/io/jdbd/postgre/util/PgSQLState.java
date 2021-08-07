package io.jdbd.postgre.util;

import reactor.util.annotation.Nullable;

public abstract class PgSQLState {

    private PgSQLState() {
    }


    public static final String UNKNOWN_STATE = "";

    public static final String TOO_MANY_RESULTS = "0100E";

    public static final String NO_DATA = "02000";

    public static final String INVALID_PARAMETER_TYPE = "07006";

    /**
     * We could establish a connection with the server for unknown reasons. Could be a network
     * problem.
     */
    public static final String CONNECTION_UNABLE_TO_CONNECT = "08001";

    public static final String CONNECTION_DOES_NOT_EXIST = "08003";

    /**
     * The server rejected our connection attempt. Usually an authentication failure, but could be a
     * configuration error like asking for a SSL connection with a server that wasn't built with SSL
     * support.
     */
    public static final String CONNECTION_REJECTED = "08004";

    /**
     * After a connection has been established, it went bad.
     */
    public static final String CONNECTION_FAILURE = "08006";
    public static final String CONNECTION_FAILURE_DURING_TRANSACTION = "08007";


    /**
     * The server sent us a response the driver was not prepared for and is either bizarre datastream
     * corruption, a driver bug, or a protocol violation on the server's part.
     */
    public static final String PROTOCOL_VIOLATION = "08P01";

    public static final String COMMUNICATION_ERROR = "08S01";

    public static final String NOT_IMPLEMENTED = "0A000";

    public static final String DATA_ERROR = "22000";
    public static final String STRING_DATA_RIGHT_TRUNCATION = "22001";
    public static final String NUMERIC_VALUE_OUT_OF_RANGE = "22003";
    public static final String BAD_DATETIME_FORMAT = "22007";
    public static final String DATETIME_OVERFLOW = "22008";
    public static final String DIVISION_BY_ZERO = "22012";
    public static final String MOST_SPECIFIC_TYPE_DOES_NOT_MATCH = "2200G";
    public static final String INVALID_PARAMETER_VALUE = "22023";

    public static final String NOT_NULL_VIOLATION = "23502";
    public static final String FOREIGN_KEY_VIOLATION = "23503";
    public static final String UNIQUE_VIOLATION = "23505";
    public static final String CHECK_VIOLATION = "23514";
    public static final String EXCLUSION_VIOLATION = "23P01";

    public static final String INVALID_CURSOR_STATE = "24000";

    public static final String TRANSACTION_STATE_INVALID = "25000";
    public static final String ACTIVE_SQL_TRANSACTION = "25001";
    public static final String NO_ACTIVE_SQL_TRANSACTION = "25P01";
    public static final String IN_FAILED_SQL_TRANSACTION = "25P02";

    public static final String INVALID_SQL_STATEMENT_NAME = "26000";
    public static final String INVALID_AUTHORIZATION_SPECIFICATION = "28000";
    public static final String INVALID_PASSWORD = "28P01";

    public static final String INVALID_TRANSACTION_TERMINATION = "2D000";

    public static final String STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL = "2F003";

    public static final String INVALID_SAVEPOINT_SPECIFICATION = "3B000";

    public static final String DEADLOCK_DETECTED = "40P01";
    public static final String SYNTAX_ERROR = "42601";
    public static final String UNDEFINED_COLUMN = "42703";
    public static final String UNDEFINED_OBJECT = "42704";
    public static final String WRONG_OBJECT_TYPE = "42809";
    public static final String NUMERIC_CONSTANT_OUT_OF_RANGE = "42820";
    public static final String DATA_TYPE_MISMATCH = "42821";
    public static final String UNDEFINED_FUNCTION = "42883";
    public static final String INVALID_NAME = "42602";
    public static final String DATATYPE_MISMATCH = "42804";
    public static final String CANNOT_COERCE = "42846";
    public static final String UNDEFINED_TABLE = "42P01";

    public static final String OUT_OF_MEMORY = "53200";
    public static final String OBJECT_NOT_IN_STATE = "55000";
    public static final String OBJECT_IN_USE = "55006";

    public static final String QUERY_CANCELED = "57014";

    public static final String SYSTEM_ERROR = "60000";
    public static final String IO_ERROR = "58030";

    public static final String UNEXPECTED_ERROR = "99999";

    public static boolean isConnectionError(final @Nullable String psqlState) {
        final boolean match;
        if (psqlState == null) {
            match = false;
        } else {
            match = CONNECTION_UNABLE_TO_CONNECT.equals(psqlState)
                    || CONNECTION_DOES_NOT_EXIST.equals(psqlState)
                    || CONNECTION_REJECTED.equals(psqlState)
                    || CONNECTION_FAILURE.equals(psqlState)
                    || CONNECTION_FAILURE_DURING_TRANSACTION.equals(psqlState);
        }
        return match;
    }


}
