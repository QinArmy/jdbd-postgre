package io.jdbd.session;

import io.jdbd.lang.NonNull;

public interface TransactionStatus extends TransactionOption {


    @NonNull
    @Override
    Isolation isolation();

    boolean inTransaction();

}
