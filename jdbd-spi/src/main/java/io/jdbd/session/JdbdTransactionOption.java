package io.jdbd.session;

import io.jdbd.JdbdException;

/**
 * <p>
 * This enum is a standard implementation of {@link TransactionStatus}
 * that {@link TransactionStatus#inTransaction()} always is false.
 * </p>
 *
 * @since 1.0
 */
enum JdbdTransactionOption implements TransactionStatus {

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


    static TransactionOption option(final Isolation isolation, final boolean readOnly) {
        final TransactionOption option;
        switch (isolation) {
            case DEFAULT:
                option = readOnly ? DEFAULT_READ : DEFAULT_WRITE;
                break;
            case READ_UNCOMMITTED:
                option = readOnly ? READ_UNCOMMITTED_READ : READ_UNCOMMITTED_WRITE;
                break;
            case READ_COMMITTED:
                option = readOnly ? READ_COMMITTED_READ : READ_COMMITTED_WRITE;
                break;
            case REPEATABLE_READ:
                option = readOnly ? REPEATABLE_READ_READ : REPEATABLE_READ_WRITE;
                break;
            case SERIALIZABLE:
                option = readOnly ? SERIALIZABLE_READ : SERIALIZABLE_WRITE;
                break;
            default:
                throw new JdbdException(String.format("unexpected %s", isolation));
        }
        return option;
    }

    private final Isolation isolation;

    private final boolean readOnly;

    JdbdTransactionOption(Isolation isolation, boolean readOnly) {
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
        // always false
        return false;
    }

    @Override
    public final String toString() {
        return String.format("%s[inTransaction:false,isolation:%s,readOnly:%s].",
                JdbdTransactionOption.class.getName(),
                this.isolation.name(),
                this.readOnly
        );
    }


}
