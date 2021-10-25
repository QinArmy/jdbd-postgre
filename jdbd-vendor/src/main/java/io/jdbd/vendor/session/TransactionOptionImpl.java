package io.jdbd.vendor.session;

import io.jdbd.session.Isolation;
import io.jdbd.session.TransactionOption;

import java.util.Objects;

public final class TransactionOptionImpl implements TransactionOption {

    public static TransactionOption option(Isolation isolation, boolean readOnly, final boolean inTransaction) {
        final TransactionOption option;
        if (inTransaction) {
            option = new TransactionOptionImpl(isolation, readOnly);
        } else {
            option = TransactionOption.option(isolation, readOnly);
        }
        return option;
    }

    private final Isolation isolation;

    private final boolean readOnly;

    TransactionOptionImpl(Isolation isolation, boolean readOnly) {
        this.isolation = isolation;
        this.readOnly = readOnly;
    }

    @Override
    public Isolation getIsolation() {
        return this.isolation;
    }

    @Override
    public boolean isReadOnly() {
        return this.readOnly;
    }

    @Override
    public boolean inTransaction() {
        return true;
    }


    @Override
    public int hashCode() {
        return Objects.hash(true, this.isolation, this.readOnly);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof TransactionOption) {
            final TransactionOption option = (TransactionOption) obj;
            match = option.inTransaction()
                    && option.getIsolation() == this.isolation
                    && option.isReadOnly() == this.readOnly;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        return String.format("TransactionOption{inTransaction:true,isolation:%s,readOnly:%s}."
                , this.isolation, this.readOnly);
    }


}
