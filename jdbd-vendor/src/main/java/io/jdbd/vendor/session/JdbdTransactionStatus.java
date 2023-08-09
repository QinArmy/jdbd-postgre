package io.jdbd.vendor.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.NonNull;
import io.jdbd.session.Isolation;
import io.jdbd.session.Option;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.TransactionStatus;

import java.util.Objects;

public enum JdbdTransactionStatus implements TransactionStatus {

    READ_UNCOMMITTED_READ(Isolation.READ_UNCOMMITTED, true),
    READ_UNCOMMITTED_WRITE(Isolation.READ_UNCOMMITTED, false),

    READ_COMMITTED_READ(Isolation.READ_COMMITTED, true),
    READ_COMMITTED_WRITE(Isolation.READ_COMMITTED, false),

    REPEATABLE_READ_READ(Isolation.REPEATABLE_READ, true),
    REPEATABLE_READ_WRITE(Isolation.REPEATABLE_READ, false),

    SERIALIZABLE_READ(Isolation.SERIALIZABLE, true),
    SERIALIZABLE_WRITE(Isolation.SERIALIZABLE, false);

    public static TransactionStatus txStatus(final Isolation isolation, final boolean readOnly,
                                             final boolean inTransaction) {
        Objects.requireNonNull(isolation);

        final TransactionStatus status;
        if (!inTransaction) {
            status = (TransactionStatus) TransactionOption.option(isolation, readOnly);
            assert !status.inTransaction();
            return status;
        }
        if (isolation == Isolation.READ_COMMITTED) {
            status = readOnly ? READ_COMMITTED_READ : READ_COMMITTED_WRITE;
        } else if (isolation == Isolation.REPEATABLE_READ) {
            status = readOnly ? REPEATABLE_READ_READ : REPEATABLE_READ_WRITE;
        } else if (isolation == Isolation.SERIALIZABLE) {
            status = readOnly ? SERIALIZABLE_READ : SERIALIZABLE_WRITE;
        } else if (isolation == Isolation.READ_UNCOMMITTED) {
            status = readOnly ? READ_UNCOMMITTED_READ : READ_UNCOMMITTED_WRITE;
        } else {
            throw new JdbdException(String.format("unexpected %s", isolation));
        }
        return status;
    }

    private final Isolation isolation;

    private final boolean readOnly;

    JdbdTransactionStatus(Isolation isolation, boolean readOnly) {
        this.isolation = isolation;
        this.readOnly = readOnly;
    }

    @NonNull
    @Override
    public final Isolation isolation() {
        return this.isolation;
    }

    @Override
    public final boolean isReadOnly() {
        return this.readOnly;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T valueOf(final Option<T> key) {
        final Object value;
        if (key == Option.IN_TRANSACTION) {
            value = Boolean.TRUE;
        } else if (key == Option.ISOLATION) {
            value = this.isolation;
        } else if (key == Option.READ_ONLY) {
            value = this.readOnly;
        } else {
            value = null;
        }
        return (T) value;
    }

    @Override
    public final boolean inTransaction() {
        //always true
        return true;
    }


    @Override
    public final String toString() {
        return String.format("%s[inTransaction:true,isolation:%s,readOnly:%s].", JdbdTransactionStatus.class.getName(),
                this.isolation.name(), this.readOnly);
    }


}
