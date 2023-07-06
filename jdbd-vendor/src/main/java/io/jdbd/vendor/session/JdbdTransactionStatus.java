package io.jdbd.vendor.session;

import io.jdbd.JdbdException;
import io.jdbd.session.Isolation;
import io.jdbd.session.TransactionOption;
import io.jdbd.session.TransactionStatus;

public enum JdbdTransactionStatus implements TransactionStatus {

    DEFAULT_READ(Isolation.DEFAULT, true),
    DEFAULT_WRITE(Isolation.DEFAULT, false),

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
        final TransactionStatus status;
        if (!inTransaction) {
            status = (TransactionStatus) TransactionOption.option(isolation, readOnly);
            assert !status.inTransaction();
            return status;
        }
        switch (isolation) {
            case DEFAULT:
                status = readOnly ? DEFAULT_READ : DEFAULT_WRITE;
                break;
            case READ_UNCOMMITTED:
                status = readOnly ? READ_UNCOMMITTED_READ : READ_UNCOMMITTED_WRITE;
                break;
            case READ_COMMITTED:
                status = readOnly ? READ_COMMITTED_READ : READ_COMMITTED_WRITE;
                break;
            case REPEATABLE_READ:
                status = readOnly ? REPEATABLE_READ_READ : REPEATABLE_READ_WRITE;
                break;
            case SERIALIZABLE:
                status = readOnly ? SERIALIZABLE_READ : SERIALIZABLE_WRITE;
                break;
            default:
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

    @Override
    public final Isolation getIsolation() {
        return this.isolation;
    }

    @Override
    public final boolean isReadOnly() {
        return this.readOnly;
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
