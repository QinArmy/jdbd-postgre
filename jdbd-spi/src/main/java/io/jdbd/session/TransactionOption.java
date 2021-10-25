package io.jdbd.session;

public interface TransactionOption {

    Isolation getIsolation();

    boolean isReadOnly();

    boolean inTransaction();


    static TransactionOption option(Isolation isolation, boolean readOnly) {
        return TransactionOptionImpl.option(isolation, readOnly);
    }

}
