package io.jdbd.session;

public interface TransactionStatus extends TransactionOption {

    boolean inTransaction();

}
