package io.jdbd.vendor.util;

import io.jdbd.lang.Nullable;
import io.jdbd.session.XaException;
import io.jdbd.session.Xid;

public abstract class XaUtils {

    protected XaUtils() {
        throw new UnsupportedOperationException();
    }


    @Nullable
    public static XaException checkXid(final @Nullable Xid xid) {
        final XaException error;
        final String bqual;
        if (xid == null) {
            error = JdbdExceptions.xidIsNull();
        } else if (!JdbdStrings.hasText(xid.getGtrid())) {
            error = JdbdExceptions.xaGtridNoText();
        } else if ((bqual = xid.getBqual()) != null && !JdbdStrings.hasText(bqual)) {
            error = JdbdExceptions.xaBqualNonNullAndNoText();
        } else {
            error = null;
        }
        return error;
    }


}
