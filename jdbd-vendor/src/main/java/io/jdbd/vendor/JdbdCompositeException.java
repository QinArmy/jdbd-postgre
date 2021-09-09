package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.vendor.util.JdbdCollections;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public class JdbdCompositeException extends JdbdNonSQLException {

    private final List<? extends Throwable> errorList;

    public JdbdCompositeException(List<? extends Throwable> errorList) {
        super(createErrorMessage(errorList));
        this.errorList = JdbdCollections.unmodifiableList(errorList);
    }

    /**
     * @return a unmodifiable list.
     */
    public List<? extends Throwable> getErrorList() {
        return this.errorList;
    }


    @Override
    public final void printStackTrace() {
        printStackTrace(System.err);
    }


    @Override
    public void printStackTrace(PrintWriter s) {
        super.printStackTrace(s);
    }

    @Nullable
    protected static Throwable getFirstError(List<? extends Throwable> errorList) {
        return errorList.isEmpty() ? null : errorList.get(0);
    }

    protected static String createErrorMessage(List<? extends Throwable> errorList) {
        if (errorList.isEmpty()) {
            throw new IllegalArgumentException("errorList is empty.");
        }
        String message;
        try (StringWriter stringWriter = new StringWriter(); PrintWriter writer = new PrintWriter(stringWriter)) {

            for (Throwable e : errorList) {
                e.printStackTrace(writer);
            }
            message = stringWriter.toString();
        } catch (IOException e) {
            message = String.format("print error stack trace failure,first error message:%s", errorList.get(0));
        }
        return message;
    }


}
