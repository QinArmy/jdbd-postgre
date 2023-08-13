package io.jdbd.session;

import io.jdbd.lang.Nullable;

public interface TransactionOption extends OptionSpec {

    @Nullable
    Isolation isolation();

    boolean isReadOnly();


    @Override
    String toString();


    static TransactionOption option(@Nullable Isolation isolation, boolean readOnly) {
        return JdbdTransactionOption.option(isolation, readOnly);
    }


    static Builder builder() {
        return JdbdTransactionOption.builder();
    }

    interface Builder {

        <T> Builder option(Option<T> key, @Nullable T value);

        /**
         * @throws IllegalArgumentException throw when <ul>
         *                                  <li>the value of {@link Option#READ_ONLY} is null</li>
         *                                  <li>{@link Option#IN_TRANSACTION} exists</li>
         *                                  </ul>
         */
        TransactionOption build() throws IllegalArgumentException;


    }//Builder

}
