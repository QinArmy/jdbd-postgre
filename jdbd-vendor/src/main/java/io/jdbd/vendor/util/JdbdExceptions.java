package io.jdbd.vendor.util;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.JdbdUnknownException;
import org.qinarmy.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public abstract class JdbdExceptions extends ExceptionUtils {

    protected JdbdExceptions() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(JdbdExceptions.class);


    public static JdbdException wrap(Throwable e) {
        JdbdException je;
        if (e instanceof JdbdException) {
            je = (JdbdException) e;
        } else if (e instanceof SQLException) {
            je = new JdbdSQLException((SQLException) e);
        } else {
            je = new JdbdUnknownException(String.format("Unknown error,%s", e.getMessage()), e);
        }
        return je;
    }

    public static JdbdException wrap(Throwable e, String format, @Nullable Object... args) {
        final String message;
        if (args == null || args.length == 0) {
            message = format;
        } else {
            message = String.format(format, args);
        }
        JdbdException je;
        if (e instanceof JdbdException) {
            je = (JdbdException) e;
        } else if (e instanceof SQLException) {
            je = new JdbdSQLException(message, (SQLException) e);
        } else {
            je = new JdbdUnknownException(message, e);
        }
        return je;
    }

    public static Throwable wrapIfNonJvmFatal(Throwable e) {
        return isJvmFatal(e) ? e : wrap(e);
    }


    public static boolean isJvmFatal(@Nullable Throwable e) {
        return e instanceof VirtualMachineError
                || e instanceof ThreadDeath
                || e instanceof LinkageError;
    }


    public static JdbdException createException(List<? extends Throwable> errorList) {
        final JdbdException e;
        if (errorList.size() == 1) {
            e = wrap(errorList.get(0));
        } else {
            e = new JdbdCompositeException(errorList, errorList.get(0).getMessage());
        }
        return e;
    }

    public static void printCompositeException(final JdbdCompositeException ce) {
        Throwable e;
        List<? extends Throwable> list = ce.getErrorList();
        final int size = list.size();
        for (int i = 0; i < size; i++) {
            e = list.get(i);
            LOG.error("JdbdCompositeException element {} : ", i, e);
        }

    }

    public static SQLException createMultiStatementError() {
        return createSyntaxError("You have an error in your SQL syntax,sql is multi statement; near ';' ");
    }

    public static SQLException createSyntaxError(String reason) {
        return new SQLException(reason, SQLStates.SYNTAX_ERROR);
    }

    public static SQLException createInvalidParameterValueError(int stmtIndex, int paramIndex) {
        String m = String.format("Invalid parameter at batch[index:%s] param[index:%s]", stmtIndex, paramIndex);
        return new SQLException(m, SQLStates.INVALID_PARAMETER_VALUE);
    }

    public static SQLException createParameterCountMatchError(int stmtIndex, int paramCount, int bindCount) {
        String m;
        if (stmtIndex == 0) {
            m = String.format("parameter count[%s] and bind count[%s] not match.", paramCount, bindCount);
        } else {
            m = String.format("Batch[index:%s] parameter count[%s] and bind count[%s] not match."
                    , stmtIndex, paramCount, bindCount);
        }
        return new SQLException(m, SQLStates.INVALID_PARAMETER_VALUE);
    }

    public static SQLException createDuplicationParameterError(int stmtIndex, int paramIndex) {
        String m;
        if (stmtIndex == 0) {
            m = String.format("parameter [index:%s] duplication.", paramIndex);
        } else {
            m = String.format("Batch[index:%s] parameter [index:%s] duplication."
                    , stmtIndex, paramIndex);
        }
        return new SQLException(m, SQLStates.INVALID_PARAMETER_VALUE);
    }

    public static SQLException createNoParameterValueError(int stmtIndex, int paramIndex) {
        String m;
        if (stmtIndex == 0) {
            m = String.format("No value specified for parameter[index:%s].", paramIndex);
        } else {
            m = String.format("Batch[index:%s] No value specified for parameter[index:%s]."
                    , stmtIndex, paramIndex);
        }
        return new SQLException(m, SQLStates.INVALID_PARAMETER_VALUE);
    }

    public static SQLException createNoAnyParamGroupError() {
        return new SQLException("Not found any parameter group.", SQLStates.INVALID_PARAMETER_VALUE);
    }


}
