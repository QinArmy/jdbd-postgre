package io.jdbd.vendor.session;

import io.jdbd.session.Xid;
import reactor.util.annotation.Nullable;

import java.util.Objects;

public class XidImpl implements Xid {


    public static XidImpl create(String gtrid, @Nullable String bqual, int formatId) {
        return new XidImpl(gtrid, bqual, formatId);
    }

    private final String gtrid;

    private final String bqual;

    private final int formatId;

    private XidImpl(String gtrid, @Nullable String bqual, int formatId) {
        this.gtrid = gtrid;
        this.bqual = bqual;
        this.formatId = formatId;
    }

    @Override
    public String getGtrid() {
        return this.gtrid;
    }

    @Nullable
    @Override
    public String getBqual() {
        return this.bqual;
    }

    @Override
    public int getFormatId() {
        return this.formatId;
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.gtrid, this.bqual, this.formatId);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Xid) {
            final Xid xid = (Xid) obj;
            final String bqual = this.bqual;
            if (bqual == null) {
                match = this.gtrid.equals(xid.getGtrid())
                        && xid.getBqual() == null
                        && this.formatId == xid.getFormatId();
            } else {
                match = this.gtrid.equals(xid.getGtrid())
                        && bqual.equals(xid.getBqual())
                        && this.formatId == xid.getFormatId();
            }
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder(140);
        builder.append(this.gtrid);
        final String bqual = this.bqual;
        builder.append(',');
        if (bqual == null) {
            builder.append(bqual);
        }
        builder.append(',')
                .append(this.formatId);
        return builder.toString();
    }
}
