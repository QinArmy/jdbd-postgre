package io.jdbd.vendor;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.vendor.util.JdbdCollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class JdbdCompositeException extends JdbdNonSQLException {

    private final List<? extends Throwable> errorList;


    public JdbdCompositeException(List<? extends Throwable> errorList, String messageFormat, Object... args) {
        super(getFirstError(errorList), messageFormat, args);
        this.errorList = JdbdCollectionUtils.unmodifiableList(new ArrayList<>(errorList));
    }

    public JdbdCompositeException(List<Throwable> errorList, boolean enableSuppression, boolean writableStackTrace
            , String messageFormat, Object... args) {
        super(getFirstError(errorList), enableSuppression, writableStackTrace, messageFormat, args);
        this.errorList = JdbdCollectionUtils.unmodifiableList(new ArrayList<>(errorList));
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


}
