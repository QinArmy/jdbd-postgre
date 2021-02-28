package io.jdbd.mysql.util;

import io.jdbd.JdbdSQLException;
import io.jdbd.LongDataReadException;
import io.jdbd.PreparedStatement;
import io.jdbd.Statement;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ErrorPacket;
import io.jdbd.mysql.protocol.MySQLFatalIoException;
import org.qinarmy.util.security.ExceptionUtils;

import java.sql.SQLException;

/**
 * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html">Server Error Message Reference</a>
 * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/client-error-reference.html">Client Error Message Reference</a>
 * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/global-error-reference.html">Global Error Message Reference</a>
 */
public abstract class MySQLExceptions extends ExceptionUtils {

    protected MySQLExceptions() {
        throw new UnsupportedOperationException();
    }

    public static int CR_PARAMS_NOT_BOUND = 2031;
    public static int CR_NO_PARAMETERS_EXISTS = 2033;
    public static int CR_INVALID_PARAMETER_NO = 2034;
    public static int CR_UNSUPPORTED_PARAM_TYPE = 2036;

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

    public static JdbdMySQLException wrapJdbdExceptionIfNeed(Throwable t) {
        JdbdMySQLException e;
        if (t instanceof JdbdMySQLException) {
            e = (JdbdMySQLException) t;
        } else {
            e = new JdbdMySQLException(t, t.getMessage());
        }
        return e;
    }

    public static JdbdSQLException createErrorPacketException(ErrorPacket error) {
        return new JdbdSQLException(createSQLException(error));
    }

    public static SQLException createSQLException(ErrorPacket error) {
        return new SQLException(error.getErrorMessage(), error.getSqlState(), error.getErrorCode());
    }

    public static JdbdSQLException createNonResultSetCommandException() {
        String message = "SQL isn't query command,please use " + Statement.class.getName() +
                ".executeQuery(String,BiFunction<ResultRow,ResultRowMeta,T>, Consumer<ResultStates>) method";
        return new JdbdSQLException(new SQLException(message));
    }

    public static JdbdSQLException createNonCommandUpdateException() {
        String message = "SQL isn't dml command,please use %s.executeUpdate() or %s.executeUpdate(String ) method";
        message = String.format(message, PreparedStatement.class.getName(), Statement.class.getName());
        return new JdbdSQLException(new SQLException(message));
    }

    public static JdbdSQLException createFatalIoException(@Nullable Throwable e, String format
            , @Nullable Object... args) {
        String message;
        if (args == null || args.length == 0) {
            message = format;
        } else {
            message = String.format(format, args);
        }
        return new JdbdSQLException(new MySQLFatalIoException(message, e));
    }

    public static JdbdSQLException createFatalIoException(String format, @Nullable Object... args) {
        return createFatalIoException(null, format, args);
    }


    public static boolean containFatalIoException(final Throwable e) {
        return containException(e, MySQLFatalIoException.class);
    }

    public static JdbdSQLException createEmptySqlException() {
        return new JdbdSQLException(createQueryIsEmptyError());
    }

    public static JdbdSQLException createMultiStatementException() {
        return new JdbdSQLException(createMultiStatementError());
    }


    public static SQLException createInvalidParameterNoError(int stmtIndex, int paramIndex) {
        String message = String.format("Invalid parameter number[%s] in statement[sequenceId:%s]."
                , paramIndex, stmtIndex);
        return new SQLException(message, null, CR_INVALID_PARAMETER_NO);
    }


    public static LongDataReadException createLongDataReadException(int stmtIndex, BindValue bindValue
            , Throwable cause) {
        LongDataReadException e;
        if (stmtIndex < 0) {
            e = new LongDataReadException(cause, "Read long data occur error at parameter[%s] MySQLType[%s]."
                    , bindValue.getParamIndex(), bindValue.getType());
        } else {
            e = new LongDataReadException(cause, "Read long data occur error at parameter[%s] MySQLType[%s] in statement[sequenceId:%s]."
                    , bindValue.getParamIndex(), bindValue.getType(), stmtIndex);
        }
        return e;
    }



    /*################################## blow create SQLException method ##################################*/

    public static SQLException createMultiStatementError() {
        return createSyntaxError("You have an error in your SQL syntax,sql is multi statement; near ';' ");
    }

    public static SQLException createSyntaxError(String message) {
        return new SQLException(message, "42000", 1149);
    }

    public static SQLException createQueryIsEmptyError() {
        return new SQLException("Query was empty", "42000", 1065);
    }

    /**
     * @param stmtIndex [negative,n] ,if single statement ,stmtIndex is negative.
     */
    public static SQLException createUnsupportedParamTypeError(int stmtIndex, BindValue bindValue) {
        Class<?> clazz = bindValue.getRequiredValue().getClass();
        String message;
        if (stmtIndex < 0) {
            message = String.format("Using unsupported param type:%s for MySQLType[%s] at (parameter:%s),please check type or value rang."
                    , clazz.getName(), bindValue.getType(), bindValue.getParamIndex());
        } else {
            message = String.format("Using unsupported param type:%s for MySQLType[%s] at (parameter:%s) in statement[sequenceId:%s],please check type or value rang."
                    , clazz.getName(), bindValue.getType(), bindValue.getParamIndex(), stmtIndex);
        }
        return new SQLException(message, null, CR_UNSUPPORTED_PARAM_TYPE);
    }

    public static SQLException createBindValueParamIndexNotMatchError(int stmtIndex, BindValue bindValue, int paramIndex) {
        String message;
        if (stmtIndex < 0) {
            message = String.format("BindValue parameter index[%s] and sql param[%s] not match."
                    , bindValue.getParamIndex(), paramIndex);
        } else {
            message = String.format("BindValue parameter index[%s] and sql param[%s] not match in statement[sequenceId:%s]"
                    , bindValue.getParamIndex(), paramIndex, stmtIndex);
        }
        return new SQLException(message, null, CR_PARAMS_NOT_BOUND);
    }

    public static SQLException createNoParametersExistsError(int stmtIndex) {
        String message;
        if (stmtIndex < 0) {
            message = "No parameters exist in the statement";
        } else {
            message = String.format("No parameters exist in the statement[sequenceId:%s]", stmtIndex);
        }

        return new SQLException(message, null, CR_NO_PARAMETERS_EXISTS);
    }

    /**
     * @param stmtIndex  [negative,n] ,if single statement ,stmtIndex is negative.
     * @param paramIndex [0,n]
     */
    public static SQLException createParamsNotBindError(int stmtIndex, int paramIndex) {
        String message;
        if (stmtIndex < 0) {
            message = String.format("No data supplied for parameters[%s] in prepared statement.", paramIndex);
        } else {
            message = String.format("No data supplied for parameters[%s] in statement[sequenceId:%s]."
                    , paramIndex, stmtIndex);
        }
        return new SQLException(message, null, CR_PARAMS_NOT_BOUND);
    }


    private static SQLException createParseError(String message) {
        return new SQLException(message, "42000", 1064);
    }


    private static SQLException createInvalidParameterError(String message) {
        return new SQLException(message, null, 2034);
    }



    /*################################## blow private static method ##################################*/


}
