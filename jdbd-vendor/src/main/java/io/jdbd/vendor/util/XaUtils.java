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


    public static String xidToString(final Xid xid) {
        final String gtrid, bqual;
        gtrid = xid.getGtrid();
        bqual = xid.getBqual();

        if (!JdbdStrings.hasText(gtrid)) {
            throw JdbdExceptions.xaGtridNoText();
        } else if (bqual != null && !JdbdStrings.hasText(bqual)) {
            throw JdbdExceptions.xaBqualNonNullAndNoText();
        }

        final StringBuilder builder = new StringBuilder(64);

        builder.append(gtrid);

        if (bqual != null) {
            builder.append(',')
                    .append(bqual);
        }
        return builder.append(',')
                .append(xid.getFormatId())
                .toString();
    }


    public static Xid xidFromString(final String xidString) {
        final String[] parts;
        parts = xidString.split(",");
        if (parts.length > 3 || parts.length < 2) {
            String m = String.format("server response unknown xid[%s]", xidString);
            throw new XaException(m, SQLStates.ER_XA_RMERR, 0, XaException.XAER_RMERR);
        } else if (!JdbdStrings.hasText(parts[0])) {
            String m = String.format("server response unknown xid[%s]", xidString);
            throw new XaException(m, SQLStates.ER_XA_RMERR, 0, XaException.XAER_RMERR);
        } else if (!JdbdStrings.hasText(parts[1])) {
            String m = String.format("server response unknown xid[%s]", xidString);
            throw new XaException(m, SQLStates.ER_XA_RMERR, 0, XaException.XAER_RMERR);
        }
    }

}
