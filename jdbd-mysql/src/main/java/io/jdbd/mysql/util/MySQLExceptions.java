package io.jdbd.mysql.util;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.protocol.MySQLFatalIoException;
import io.jdbd.mysql.protocol.client.ErrorPacket;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.StaticStatement;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdExceptions;

import java.sql.SQLException;

/**
 * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html">Server Error Message Reference</a>
 * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html">Client Error Message Reference</a>
 * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/global-error-reference.html">Global Error Message Reference</a>
 */
public abstract class MySQLExceptions extends JdbdExceptions {


    public static final short CR_PARAMS_NOT_BOUND = 2031;
    public static final short CR_NO_PARAMETERS_EXISTS = 2033;
    public static final short CR_INVALID_PARAMETER_NO = 2034;
    public static final short CR_UNSUPPORTED_PARAM_TYPE = 2036;


    public static JdbdException wrap(final Throwable cause) {
        final JdbdException e;
        if (cause instanceof JdbdException) {
            e = (JdbdException) cause;
        } else if (isByteBufOutflow(cause)) {
            e = netPacketTooLargeError(cause);
        } else {
            e = new JdbdException(String.format("Unknown error,%s", cause.getMessage()), cause);
        }
        return e;
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

    public static MySQLJdbdException wrapJdbdExceptionIfNeed(Throwable t) {
        MySQLJdbdException e;
        if (t instanceof MySQLJdbdException) {
            e = (MySQLJdbdException) t;
        } else {
            e = new MySQLJdbdException(t, t.getMessage());
        }
        return e;
    }

    public static JdbdException createErrorPacketException(ErrorPacket error) {
        return new JdbdException(error.getErrorMessage(), error.getSqlState(), error.getErrorCode());
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html">Mapping MySQL Error Numbers to JDBC SQLState Codes</a>
     */
    public static SQLException createSQLException(ErrorPacket error) {
        return new SQLException(error.getErrorMessage(), error.getSqlState(), error.getErrorCode());
    }

    public static JdbdSQLException createNonResultSetCommandException() {
        String message = "SQL isn't query command,please use " + StaticStatement.class.getName() +
                ".executeQuery(String,BiFunction<ResultRow,ResultRowMeta,T>, Consumer<ResultStates>) method";
        return new JdbdSQLException(new SQLException(message));
    }

    public static JdbdSQLException createNonCommandUpdateException() {
        String message = "SQL isn't dml command,please use %s.executeUpdate() or %s.executeUpdate(String ) method";
        message = String.format(message, PreparedStatement.class.getName(), StaticStatement.class.getName());
        return new JdbdSQLException(new SQLException(message));
    }

    public static JdbdException createFatalIoException(String message, @Nullable Throwable cause) {
        final JdbdException e;
        if (cause == null) {
            e = new JdbdException(message);
        } else {
            e = new JdbdException(message, cause);
        }
        return e;
    }


    public static boolean containFatalIoException(final Throwable e) {
        return containException(e, MySQLFatalIoException.class);
    }

    public static JdbdException sqlIsEmpty() {
        return new JdbdException("Query was empty", "42000", 1065);
    }


    public static JdbdException createInvalidParameterNoError(int stmtIndex, int paramIndex) {
        String message = String.format("Invalid parameter number[%s] in statement[sequenceId:%s]."
                , paramIndex, stmtIndex);
        return new JdbdException(message, null, CR_INVALID_PARAMETER_NO);
    }


    public static LongDataReadException createLongDataReadException(int stmtIndex, ParamValue bindValue
            , Throwable cause) {
        final String m;
        if (stmtIndex < 0) {
            m = String.format("Read long data occur error at parameter[%s] DataType[%s]."
                    , bindValue.getIndex(), bindValue.getType());
        } else {
            m = String.format("Read long data occur error at parameter[%s] DataType[%s] in statement[sequenceId:%s]."
                    , bindValue.getIndex(), bindValue.getType(), stmtIndex);
        }
        return new LongDataReadException(m, cause);
    }



    /*################################## blow create SQLException method ##################################*/


    public static SQLException createSyntaxError(String message) {
        return createSyntaxError(message, null);
    }

    public static SQLException createSyntaxError(String message, @Nullable Throwable cause) {
        return new SQLException(message, MySQLStates.SYNTAX_ERROR, MySQLCodes.ER_SYNTAX_ERROR, cause);
    }

    public static JdbdException createQueryIsEmptyError() {
        return new JdbdException("Query was empty", MySQLStates.SYNTAX_ERROR, 1065);
    }

    /**
     * @param stmtIndex [negative,n] ,if single statement ,stmtIndex is negative.
     */
    public static JdbdException createUnsupportedParamTypeError(final int stmtIndex, final ParamValue value) {
        Class<?> clazz = value.getNonNull().getClass();
        String message;
        if (stmtIndex < 0) {
            message = String.format("Using unsupported param type:%s for MySQLType[%s] at (parameter:%s),please check type or value rang."
                    , clazz.getName(), value.getType(), value.getIndex());
        } else {
            message = String.format("Using unsupported param type:%s for MySQLType[%s] at (parameter:%s) in statement[sequenceId:%s],please check type or value rang."
                    , clazz.getName(), value.getType(), value.getIndex(), stmtIndex);
        }
        return new JdbdException(message, null, MySQLCodes.CR_UNSUPPORTED_PARAM_TYPE);
    }


    public static JdbdException bindValueParamIndexNotMatchError(int stmtIndex, ParamValue paramValue,
                                                                 int paramIndex) {
        String message;
        if (stmtIndex < 0) {
            message = String.format("BindValue parameter index[%s] and sql param[%s] not match."
                    , paramValue.getIndex(), paramIndex);
        } else {
            message = String.format("BindValue parameter index[%s] and sql param[%s] not match in statement[sequenceId:%s]"
                    , paramValue.getIndex(), paramIndex, stmtIndex);
        }
        return new JdbdException(message, null, CR_PARAMS_NOT_BOUND);
    }

    public static JdbdException createNoParametersExistsError(int stmtIndex) {
        String message;
        if (stmtIndex < 0) {
            message = "No parameters exist in the statement";
        } else {
            message = String.format("No parameters exist in the statement[sequenceId:%s]", stmtIndex);
        }

        return new JdbdException(message, null, CR_NO_PARAMETERS_EXISTS);
    }

    /**
     * @param stmtIndex  [negative,n] ,if single statement ,stmtIndex is negative.
     * @param paramIndex [0,n]
     */
    public static JdbdException createParamsNotBindError(int stmtIndex, int paramIndex) {
        String message;
        if (stmtIndex < 0) {
            message = String.format("No data supplied for parameters[%s] in prepared statement.", paramIndex);
        } else {
            message = String.format("No data supplied for parameters[%s] in statement[sequenceId:%s]."
                    , paramIndex, stmtIndex);
        }
        return new JdbdException(message, null, CR_PARAMS_NOT_BOUND);
    }


    public static JdbdException createNetPacketTooLargeException(int maxAllowedPayload) {
        String message = String.format("sql length larger than %s[%s]"
                , MyKey.maxAllowedPacket, maxAllowedPayload);
        return new JdbdException(message, netPacketTooLargeError(null));
    }


    public static JdbdSQLException createTypeNotMatchException(int stmtIndex, MySQLType mySQLType
            , ParamValue paramValue) {
        return createTypeNotMatchException(stmtIndex, mySQLType, paramValue, null);
    }

    public static JdbdException createTypeNotMatchException(int stmtIndex, MySQLType mySQLType,
                                                            ParamValue paramValue, @Nullable Throwable cause) {
        String message;
        if (stmtIndex < 0) {
            message = String.format("Bind parameter[%s] MySQLType[%s] and JavaType[%s] value not match."
                    , paramValue.getIndex()
                    , mySQLType
                    , paramValue.getNonNull().getClass().getName());
        } else {
            message = String.format(
                    "Parameter Group[%s] Bind parameter[%s] MySQLType[%s] and JavaType[%s] value not match."
                    , stmtIndex
                    , paramValue.getIndex()
                    , mySQLType
                    , paramValue.getNonNull().getClass().getName());
        }
        return new JdbdException(createTruncatedWrongValueForField(message, cause));
    }

    public static JdbdException createDurationRangeException(int stmtIndex, MySQLType mySQLType
            , ParamValue paramValue) {
        String message;
        if (stmtIndex < 0) {
            message = String.format(
                    "Bind parameter[%s] MySQLType[%s] Duration[%s] beyond [-838:59:59,838:59:59]"
                    , paramValue.getIndex(), mySQLType, paramValue.get());
        } else {
            message = String.format(
                    "Parameter Group[%s] Bind parameter[%s] MySQLType[%s] Duration[%s] beyond [-838:59:59,838:59:59]"
                    , stmtIndex, paramValue.getIndex(), mySQLType, paramValue.get());
        }
        return new JdbdException(createTruncatedWrongValue(message, null));
    }

    public static JdbdException createNotSupportScaleException(int stmtIndex, MySQLType mySQLType
            , ParamValue paramValue) {
        final String message;
        if (stmtIndex < 0) {
            message = String.format("Bind parameter[%s] is MySQLType[%s],not support fraction."
                    , paramValue.getIndex(), mySQLType);
        } else {
            message = String.format("Parameter Group[%s] Bind parameter[%s] is MySQLType[%s],not support fraction."
                    , stmtIndex, paramValue.getIndex(), mySQLType);
        }
        return new JdbdException(createDataOutOfRangeError(message, null));
    }

    public static JdbdException createDataTooLongException(int stmtIndex, MySQLType mySQLType
            , ParamValue paramValue) {
        final String message;
        if (stmtIndex < 0) {
            message = String.format("Bind parameter[%s] MySQLType[%s] too long."
                    , paramValue.getIndex(), mySQLType);
        } else {
            message = String.format("Parameter Group[%s] Bind parameter[%s] MySQLType[%s] too long."
                    , stmtIndex, paramValue.getIndex(), mySQLType);
        }
        return new JdbdException(createDataTooLongError(message, null));
    }

    public static JdbdException createNumberRangErrorException(int stmtIndex, MySQLType mySQLType
            , ParamValue bindValue, @Nullable Throwable cause, Number lower, Number upper) {
        final String message;
        if (stmtIndex < 0) {
            message = String.format("Bind parameter[%s] MySQLType[%s] beyond rang[%s,%s]."
                    , bindValue.getIndex(), mySQLType, lower, upper);
        } else {
            message = String.format("Parameter Group[%s] Bind parameter[%s] MySQLType[%s] out range[%s,%s]."
                    , stmtIndex, bindValue.getIndex(), mySQLType, lower, upper);
        }
        return new JdbdException(createDataOutOfRangeError(message, cause));
    }

    public static JdbdException createNumberRangErrorException(int stmtIndex, MySQLType mySQLType
            , ParamValue bindValue, Number lower, Number upper) {
        return createNumberRangErrorException(stmtIndex, mySQLType, bindValue, null, lower, upper);
    }

    public static JdbdException createWrongArgumentsException(int stmtIndex, MySQLType mySQLType
            , ParamValue paramValue, @Nullable Throwable cause) {
        String message;
        if (stmtIndex < 0) {
            message = String.format("Bind parameter[%s] MySQLType[%s] param type[%s] error."
                    , paramValue.getIndex(), mySQLType, paramValue.getNonNull().getClass().getName());
        } else {
            message = String.format("Parameter Group[%s] Bind parameter[%s] MySQLType[%s]  param type[%s] error."
                    , stmtIndex, paramValue.getIndex(), mySQLType
                    , paramValue.getNonNull().getClass().getName());
        }
        return new JdbdException(createWrongArgumentsError(message, cause));
    }

    public static SQLException createDataOutOfRangeError(String message, @Nullable Throwable cause) {
        return new SQLException(message
                , MySQLCodes.ERROR_TO_SQL_STATES_MAP.get(MySQLCodes.ER_DATA_OUT_OF_RANGE)
                , MySQLCodes.ER_DATA_OUT_OF_RANGE, cause);

    }

    public static JdbdException createTruncatedWrongValue(String message, @Nullable Throwable cause) {
        final String sqlStates;
        sqlStates = MySQLCodes.ERROR_TO_SQL_STATES_MAP.get(MySQLCodes.ER_TRUNCATED_WRONG_VALUE);
        final JdbdException e;
        if (cause == null) {
            e = new JdbdException(message, sqlStates, MySQLCodes.ER_TRUNCATED_WRONG_VALUE);
        } else {
            e = new JdbdException(message, sqlStates, MySQLCodes.ER_TRUNCATED_WRONG_VALUE, cause);
        }
        return e;
    }

    public static SQLException createTruncatedWrongValueForField(String message, @Nullable Throwable cause) {
        return new SQLException(message
                , MySQLCodes.ERROR_TO_SQL_STATES_MAP.get(MySQLCodes.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD)
                , MySQLCodes.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, cause);
    }

    public static SQLException createWrongArgumentsError(String message, @Nullable Throwable cause) {
        return new SQLException(message
                , MySQLCodes.ERROR_TO_SQL_STATES_MAP.get(MySQLCodes.ER_WRONG_ARGUMENTS)
                , MySQLCodes.ER_WRONG_ARGUMENTS, cause);
    }


    public static SQLException createDataTooLongError(String message, @Nullable Throwable cause) {
        return new SQLException(message
                , MySQLCodes.ERROR_TO_SQL_STATES_MAP.get(MySQLCodes.ER_DATA_TOO_LONG)
                , MySQLCodes.ER_DATA_TOO_LONG, cause);
    }

    public static JdbdException netPacketTooLargeError(@Nullable Throwable cause) {
        final JdbdException e;
        if (cause == null) {
            e = new JdbdException("Got a packet bigger than 'max_allowed_packet' bytes",
                    MySQLStates.COMMUNICATION_LINK_FAILURE, MySQLCodes.ER_NET_PACKET_TOO_LARGE);
        } else {
            e = new JdbdException("Got a packet bigger than 'max_allowed_packet' bytes",
                    MySQLStates.COMMUNICATION_LINK_FAILURE, MySQLCodes.ER_NET_PACKET_TOO_LARGE, cause);
        }
        return e;
    }



    /*################################## blow private static method ##################################*/


}
