package io.jdbd.result;

/**
 * @see ResultRow
 * @see ResultStates
 * @see ResultRowMeta#getResultIndex()
 */
public interface Result {


    /**
     * @return index of this Query/Update result, based zero.
     */
    int getResultIndex();

}
