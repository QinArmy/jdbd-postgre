package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.vendor.util.JdbdCollections;

import java.util.ArrayList;
import java.util.List;

public class JdbdCompositeException extends JdbdNonSQLException {

    private final List<? extends Throwable> errorList;

    public JdbdCompositeException(List<? extends Throwable> errorList) {
        super("");
        this.errorList = JdbdCollections.unmodifiableList(errorList);
    }

    public JdbdCompositeException(List<? extends Throwable> errorList, String messageFormat, Object... args) {
        super(createErrorMessage(messageFormat, errorList), messageFormat, args);
        this.errorList = JdbdCollections.unmodifiableList(new ArrayList<>(errorList));
    }

    public JdbdCompositeException(List<Throwable> errorList, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(getFirstError(errorList), enableSuppression, writableStackTrace, messageFormat, args);
        this.errorList = JdbdCollections.unmodifiableList(new ArrayList<>(errorList));
    }

    /**
     * @return a unmodifiable list.
     */
    public List<? extends Throwable> getErrorList() {
        return this.errorList;
    }

    @Nullable
    protected static Throwable getFirstError(List<? extends Throwable> errorList) {
        return errorList.isEmpty() ? null : errorList.get(0);
    }

    protected static String createErrorMessage(String message, List<? extends Throwable> errorList) {
        StringBuilder builder = new StringBuilder(message.length() + 15 * errorList.size())
                .append(message)
                .append(":\n");
        int count = 0;
        String m;
        for (Throwable throwable : errorList) {
            if (count > 0) {
                builder.append("\n; ");
            }
            m = throwable.getMessage();
            if (m != null) {
                builder.append(m);
            }
            count++;
        }
        return builder.toString();
    }


}
