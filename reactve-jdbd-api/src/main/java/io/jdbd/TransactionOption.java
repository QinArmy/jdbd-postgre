package io.jdbd;

public interface TransactionOption {

    Isolation getIsolation();

    boolean isReadOnly();

    boolean isAutoCommit();

    static TransactionOption build(Isolation isolation, boolean readOnly){
        return new TransactionOptionImpl(isolation, readOnly, readOnly);
    }

    static TransactionOption build(Isolation isolation, boolean readOnly, boolean autoCommit) {
        return new TransactionOptionImpl(isolation, readOnly, autoCommit);
    }

}
