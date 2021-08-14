package io.jdbd.result;

/**
 * @see ResultRow
 * @see ResultState
 * @see ResultRowMeta#getResultIndex()
 */
public interface Result {


    /**
     * @return index of this Query/Update result, based zero.
     */
    default int getResultIndex() {
        throw new UnsupportedOperationException();
    }

}
