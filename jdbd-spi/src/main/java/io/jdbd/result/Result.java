package io.jdbd.result;

/**
 * @see ResultRow
 * @see ResultStates
 * @see ResultRowMeta#getResultNo()
 */
public interface Result {


    /**
     * @return number of this Query/Update result, based one.
     */
    int getResultNo();

}
