package io.jdbd.result;


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


    boolean isLastResult();

    default long getRowCount() {
        throw new UnsupportedOperationException();
    }

    default boolean hasColumn() {
        throw new UnsupportedOperationException();
    }

    default int getWarnings() {
        throw new UnsupportedOperationException();
    }

}
