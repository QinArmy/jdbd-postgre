package io.jdbd.session;

final class TransactionOptionImpl implements TransactionOption {

    private final Isolation isolation;

    private final boolean readOnly;

    private final boolean autoCommit;

    TransactionOptionImpl(Isolation isolation, boolean readOnly, boolean autoCommit) {
        this.isolation = isolation;
        this.readOnly = readOnly;
        this.autoCommit = autoCommit;
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
    public boolean isAutoCommit() {
        return this.autoCommit;
    }
}
