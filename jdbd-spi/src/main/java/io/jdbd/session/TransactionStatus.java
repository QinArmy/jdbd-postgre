package io.jdbd.session;

public interface TransactionStatus extends TransactionOption {

    /**
     * @return never {@link Isolation#DEFAULT}
     */
    @Override
    Isolation getIsolation();

    boolean inTransaction();

}
