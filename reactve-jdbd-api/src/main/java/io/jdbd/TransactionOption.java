package io.jdbd;

public interface TransactionOption {

    Isolation getIsolation();

    boolean isReadOnly();

    boolean autoCommit();

}
