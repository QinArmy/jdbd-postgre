package io.jdbd.session;

public interface TransactionOption {

    Isolation getIsolation();

    boolean isReadOnly();


    static TransactionOption option(Isolation isolation, boolean readOnly) {
        return JdbdTransactionOption.option(isolation, readOnly);
    }

}
