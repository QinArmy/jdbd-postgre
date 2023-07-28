package io.jdbd.result;


import io.jdbd.session.Option;

public interface ResultStates extends Result {

    boolean supportInsertId();

    boolean inTransaction();

    long getAffectedRows();

    long getInsertId();

    /**
     * @return success info(maybe contain warning info)
     */
    String getMessage();

    boolean hasMoreResult();

    boolean hasMoreFetch();

    default long getRowCount() {
        throw new UnsupportedOperationException();
    }

    default boolean hasColumn() {
        throw new UnsupportedOperationException();
    }

    default int getWarnings() {
        throw new UnsupportedOperationException();
    }


    <T> T valueOf(Option<T> option);

}
