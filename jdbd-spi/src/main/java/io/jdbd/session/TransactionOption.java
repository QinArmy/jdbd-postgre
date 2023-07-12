package io.jdbd.session;

import io.jdbd.lang.Nullable;

public interface TransactionOption {

    @Nullable
    Isolation getIsolation();

    boolean isReadOnly();


    static TransactionOption option(@Nullable Isolation isolation, boolean readOnly) {
        return JdbdTransactionOption.option(isolation, readOnly);
    }

}
