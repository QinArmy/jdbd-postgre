package io.jdbd.vendor.util;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.meta.SQLType;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultStates;
import io.jdbd.session.SavePoint;
import io.jdbd.statement.OutParameter;
import io.jdbd.statement.PreparedStatement;
import io.jdbd.statement.Statement;
import io.jdbd.vendor.JdbdCompositeException;
import io.jdbd.vendor.stmt.NamedValue;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

public abstract class JdbdExceptions {

    protected JdbdExceptions() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(JdbdExceptions.class);


    public static JdbdException wrap(Throwable e) {
        JdbdException je;
        if (e instanceof JdbdException) {
            je = (JdbdException) e;
        } else {
            je = new JdbdException(String.format("Unknown error,%s", e.getMessage()), e);
        }
        return je;
    }


    public static JdbdException unexpectedEnum(Enum<?> e) {
        return new JdbdException(String.format("unexpected enum %s", e));
    }

    public static JdbdException queryMapFuncError(Function<CurrentRow, ?> function) {
        String m = String.format("query map function %s couldn't return null or %s ",
                function, CurrentRow.class.getName());
        return new JdbdException(m);
    }

    public static NullPointerException queryMapFuncIsNull() {
        return new NullPointerException("query map function must non-null");
    }

    public static NullPointerException statesConsumerIsNull() {
        return new NullPointerException(String.format("%s consumer must non-null", ResultStates.class.getName()));
    }

    public static NullPointerException dataTypeIsNull() {
        return new NullPointerException("dataType must non-null");
    }

    public static JdbdException dontSupportDataType(DataType dataType, String database) {
        return new JdbdException(String.format("%s don't support %s[%s]", database, DataType.class.getName(), dataType));
    }

    public static RuntimeException stmtVarNameHaveNoText(@Nullable String name) {
        final RuntimeException error;
        if (name == null) {
            error = new NullPointerException("statement variable name must non-null.");
        } else {
            error = new IllegalArgumentException("statement variable name must have text.");
        }
        return error;
    }

    public static JdbdException dontSupportImporter(String database) {
        return new JdbdException(String.format("%s don't support importer.", database));
    }


    public static JdbdException dontSupportExporter(String database) {
        return new JdbdException(String.format("%s don't support exporter.", database));
    }


    public static String safeClassName(@Nullable Object value) {
        return value == null ? "" : value.getClass().getName();
    }

    public static JdbdException dontSupportJavaType(Object indexOrName, @Nullable Object value, String database) {
        String m;
        m = String.format("%s don't support java type[%s] at index/name[%s]", database, safeClassName(value),
                indexOrName);
        return new JdbdException(m);
    }

    public static JdbdException dontSupportOutParameter(Object indexOrName, Class<? extends Statement> stmtClass,
                                                        String database) {
        String m = String.format("%s %s don't support %s at index/name %s.", database, stmtClass.getName(),
                OutParameter.class.getName(), indexOrName);
        return new JdbdException(m);
    }

    public static JdbdException stmtVarDuplication(String name) {
        return new JdbdException(String.format("statement variable[%s] duplication.", name));
    }

    public static JdbdException wrap(Throwable e, String format, @Nullable Object... args) {
        final String message;
        if (args == null || args.length == 0) {
            message = format;
        } else {
            message = String.format(format, args);
        }
        final JdbdException je;
        if (e instanceof JdbdException) {
            je = (JdbdException) e;
        } else if (e instanceof SQLException) {
            je = new JdbdSQLException(message, (SQLException) e);
        } else if (e instanceof IndexOutOfBoundsException && isByteBufOutflow(e)) {
            je = new JdbdSQLException(tooLargeObject(e));
        } else {
            je = new JdbdUnknownException(message, e);
        }
        return je;
    }

    public static JdbdException dontSupportMultiStmt() {
        return new JdbdException("current session don't support multi-statement.",
                SQLStates.SYNTAX_ERROR, 0);
    }

    public static Throwable wrapForMessage(final Throwable e) {
        final Throwable error;
        if (e instanceof IndexOutOfBoundsException && isByteBufOutflow(e)) {
            error = new JdbdSQLException(tooLargeObject(e));
        } else {
            error = wrapIfNonJvmFatal(e);
        }
        return error;
    }

    public static boolean isByteBufOutflow(final Throwable e) {
        if (!(e instanceof IndexOutOfBoundsException)) {
            return false;
        }
        final String bufClassName = "io.netty.buffer.AbstractByteBuf";
        final String bufClassPrefix = "io.netty.buffer.";
        boolean match = false;
        for (StackTraceElement se : e.getStackTrace()) {
            final String className = se.getClassName();
            if (className.equals(bufClassName)
                    || className.startsWith(bufClassPrefix)) {
                match = true;
                break;
            }
        }
        return match;
    }

    public static Throwable wrapIfNonJvmFatal(Throwable e) {
        return isJvmFatal(e) ? e : wrap(e);
    }


    public static boolean isJvmFatal(@Nullable Throwable e) {
        return e instanceof VirtualMachineError
                || e instanceof LinkageError;
    }

    public static UnsupportedBindJavaTypeException notSupportBindJavaType(Class<?> notSupportType) {
        return new UnsupportedBindJavaTypeException(notSupportType);
    }


    public static JdbdException createException(List<? extends Throwable> errorList) {
        final JdbdException e;
        if (errorList.size() == 1) {
            e = wrap(errorList.get(0));
        } else {
            e = new JdbdCompositeException(errorList);
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

    public static JdbdException cannotReuseStatement(Class<? extends Statement> stmtClass) {
        return new JdbdException(String.format("Can't reuse %s .", stmtClass.getName()));
    }

    public static StatementClosedException preparedStatementClosed() {
        return new StatementClosedException(String.format("%s have closed.", PreparedStatement.class.getName()));
    }


    public static JdbdException multiStmtNoSql() {
        return new JdbdException("MultiStatement no sql,should invoke addStatement(String) method.");
    }

    public static JdbdSQLException noReturnColumn() {
        return new JdbdSQLException(new SQLException("No return column"));
    }

    public static JdbdException invalidParameterValue(int stmtIndex, int paramIndex) {
        String m;
        if (stmtIndex == 0) {
            m = String.format("Invalid parameter at  param[index:%s]", paramIndex);
        } else {
            m = String.format("Invalid parameter at batch[index:%s] param[index:%s]", stmtIndex, paramIndex);
        }
        return new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdSQLException beyondFirstParamGroupRange(int indexBasedZero, int firstGroupSize) {
        String m = String.format("bind index[%s] beyond first param group range [0,%s) ."
                , indexBasedZero, firstGroupSize);
        return new JdbdSQLException(new SQLException(m, SQLStates.INVALID_PARAMETER_VALUE));
    }


    public static JdbdException notMatchWithFirstParamGroupCount(int stmtIndex, int paramCount, int firstGroupSize) {
        final String m;
        if (stmtIndex == 0) {
            m = String.format("Param count[%s] and first group param count[%s] not match."
                    , paramCount, firstGroupSize);
        } else {
            m = String.format("Group[index:%s] param count[%s] and first group param count[%s] not match."
                    , stmtIndex, paramCount, firstGroupSize);
        }
        return new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdException parameterCountMatch(int batchIndex, int paramCount, int bindCount) {
        String m;
        if (batchIndex < 0) {
            m = String.format("parameter count[%s] and bind count[%s] not match.", paramCount, bindCount);
        } else {
            m = String.format("Batch[index:%s] parameter count[%s] and bind count[%s] not match."
                    , batchIndex, paramCount, bindCount);
        }
        return new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdException duplicationParameter(int stmtIndex, int paramIndex) {
        String m;
        if (stmtIndex == 0) {
            m = String.format("parameter [index:%s] duplication.", paramIndex);
        } else {
            m = String.format("Batch[index:%s] parameter [index:%s] duplication."
                    , stmtIndex, paramIndex);
        }
        return new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdException noParameterValue(int stmtIndex, int paramIndex) {
        String m;
        if (stmtIndex == 0) {
            m = String.format("No value specified for parameter[index:%s].", paramIndex);
        } else {
            m = String.format("Batch[index:%s] No value specified for parameter[index:%s]."
                    , stmtIndex, paramIndex);
        }
        return new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdException noAnyParamGroupError() {
        return new JdbdException("Not found any parameter group.", SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdException noInvokeAddBatch() {
        return new JdbdException("Not invoke addBatch()", SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdException unknownSavePoint(SavePoint savePoint) {
        return new JdbdException(String.format("unknown %s %s", SavePoint.class.getName(), savePoint));
    }

    public static JdbdSQLException batchAsMultiNonSupportFetch() {
        return new JdbdSQLException(
                new SQLException("executeBatchAsMulti() not support fetch.", SQLStates.INVALID_PARAMETER_VALUE));
    }


    public static JdbdException outOfTypeRange(final int batchIndex, final Value value) {
        return outOfTypeRange(batchIndex, value, null);

    }

    public static JdbdException outOfTypeRange(final int batchIndex, final Value value, final @Nullable Throwable cause) {
        String m;
        if (batchIndex < 0) {
            m = String.format("parameter[%s] value out of number range for %s",
                    getValueLabel(value), value.getType());
        } else {
            m = String.format("batch[%s] parameter[%s] value out of number range for %s",
                    batchIndex, getValueLabel(value), value.getType());
        }
        final JdbdException e;
        if (cause == null) {
            e = new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0);
        } else {
            e = new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0, cause);
        }
        return e;

    }

    public static JdbdException beyondMessageLength(int batchIndex, ParamValue bindValue) {
        String m;
        if (batchIndex < 0) {
            m = String.format("parameter[%s] too long so beyond message rest length"
                    , bindValue.getIndex());
        } else {
            m = String.format("batch[%s] parameter[%s] too long so beyond message rest length"
                    , batchIndex, bindValue.getIndex());
        }
        return new JdbdException(m);
    }

    public static JdbdException tooLargeObject() {
        return new JdbdException("Object too large,beyond message length.");
    }

    public static JdbdException tooLargeObject(Throwable e) {
        return new JdbdException("Object too large,beyond message length.", e);
    }

    public static LocalFileException localFileWriteError(int batchIndex, SQLType sqlType
            , ParamValue bindValue, Throwable e) {
        Path path = (Path) bindValue.getNonNull();
        String m;
        if (batchIndex < 0) {
            m = String.format("parameter[%s] path[%s] to sql type[%s]"
                    , bindValue.getIndex(), path, sqlType);
        } else {
            m = String.format("batch[%s] parameter[%s] path[%s] to sql type[%s]"
                    , batchIndex, bindValue.getIndex(), bindValue.get(), sqlType);
        }
        throw new LocalFileException(path, m, e);
    }

    /**
     * @param batchIndex negative:single stmt;not negative representing batch index of batch operation.
     */
    public static JdbdException createNonSupportBindSqlTypeError(int batchIndex, final Value bindValue) {
        final String m;
        if (batchIndex < 0) {
            m = String.format("parameter[%s] javaType[%s] bind to sql type[%s] not supported."
                    , getValueLabel(bindValue)
                    , bindValue.getNonNull().getClass().getName()
                    , bindValue.getType());
        } else {
            m = String.format("batch[%s] parameter[%s] javaType[%s] bind to sql type[%s] not supported."
                    , batchIndex
                    , getValueLabel(bindValue)
                    , bindValue.getNonNull().getClass().getName()
                    , bindValue.getType());
        }
        return new JdbdException(m, SQLStates.INVALID_PARAMETER_VALUE, 0);
    }

    public static JdbdSQLException notSupportClientCharset(final Charset charset) {
        String m = String.format("client charset[%s] isn't supported,because %s encode ASCII to multi bytes."
                , charset.name(), charset.name());
        throw new JdbdSQLException(new SQLException(m));
    }

    public static JdbdSQLException transactionExistsRejectStart(Object sessionId) {
        String m;
        m = String.format("Session[%s] in transaction ,reject start a new transaction before commit or rollback."
                , sessionId);
        throw new JdbdSQLException(new SQLException(m));
    }

    public static JdbdSQLException transactionExistsRejectSet(Object sessionId) {
        String m;
        m = String.format("Session[%s] in transaction ,reject set transaction characteristic before commit or rollback."
                , sessionId);
        throw new JdbdSQLException(new SQLException(m));
    }


    public static JdbdXaException xaInvalidFlagForStart(final int flags) {
        return xaInvalidFlag(flags, "start(Xid xid,int flags)");
    }

    public static JdbdXaException xaInvalidFlagForEnd(final int flags) {
        return xaInvalidFlag(flags, "end(Xid xid,int flags)");
    }

    public static JdbdXaException xaInvalidFlagForRecover(final int flags) {
        return xaInvalidFlag(flags, "recover(int flags)");
    }

    public static JdbdXaException xaGtridNoText() {
        return new JdbdXaException("gtrid of xid must have text.", SQLStates.ER_XAER_NOTA, JdbdXaException.XAER_NOTA);
    }

    public static JdbdXaException xaGtridBeyond64Bytes() {
        return new JdbdXaException("bytes length of gtrid beyond 64 bytes.", SQLStates.ER_XAER_NOTA, JdbdXaException.XAER_NOTA);
    }

    public static JdbdXaException xaBqualBeyond64Bytes() {
        return new JdbdXaException("bytes length of bqual beyond 64 bytes.", SQLStates.ER_XAER_NOTA, JdbdXaException.XAER_NOTA);
    }




    /*################################## blow protected method ##################################*/


    protected static Object getValueLabel(Value value) {
        final Object paramLabel;
        if (value instanceof ParamValue) {
            paramLabel = ((ParamValue) value).getIndex();
        } else if (value instanceof NamedValue) {
            paramLabel = ((NamedValue) value).getName();
        } else {
            throw new IllegalArgumentException(String.format("Unknown %s type[%s]", Value.class.getName(), value));
        }
        return paramLabel;
    }


    private static JdbdXaException xaInvalidFlag(final int flags, final String method) {
        String m = String.format("XA invalid flag[%s] for method %s", Integer.toBinaryString(flags), method);
        return new JdbdXaException(m, SQLStates.ER_XAER_INVAL, JdbdXaException.XAER_INVAL);
    }


}
