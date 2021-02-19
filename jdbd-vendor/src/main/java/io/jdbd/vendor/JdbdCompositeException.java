package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.vendor.util.JdbdCollectionUtils;

import java.util.Collections;
import java.util.List;

public class JdbdCompositeException extends JdbdNonSQLException {

    private final List<Throwable> errorList;

    public JdbdCompositeException(String messageFormat, Object... args) {
        super(messageFormat, args);
        this.errorList = Collections.emptyList();
    }

    public JdbdCompositeException(List<Throwable> errorList, String messageFormat, Object... args) {
        super(getFirstError(errorList), messageFormat, args);
        this.errorList = JdbdCollectionUtils.unmodifiableList(errorList);
    }

    public JdbdCompositeException(List<Throwable> errorList, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(getFirstError(errorList), enableSuppression, writableStackTrace, messageFormat, args);
        this.errorList = JdbdCollectionUtils.unmodifiableList(errorList);
    }

    /**
     * @return a unmodifiable list.
     */
    public List<Throwable> getErrorList() {
        return this.errorList;
    }

    @Nullable
    protected static Throwable getFirstError(List<Throwable> errorList) {
        return errorList.isEmpty() ? null : errorList.get(0);
    }


}
