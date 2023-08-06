package io.jdbd.result;


import io.jdbd.session.Option;

/**
 * <p>
 * The interface representing the states of the result of sql statement (eg: SELECT/INSERT/UPDATE/DELETE).
 *     <ul>
 *         <li>If {@link #hasColumn()} is true ,then this instance representing the terminator of query result (eg: SELECT command)</li>
 *         <li>Else this instance representing the update result (eg: INSERT/UPDATE/DELETE command)</li>
 *     </ul>
 * </p>
 * <p>
 *  The instance of this interface always is the last item in the {@link OrderedFlux} of same query result.
 * </p>
 * <p>
 * The {@link #getResultNo()} of this interface always return same value with {@link ResultRowMeta} in same query result.
 * </p>
 *
 * @see ResultRowMeta
 * @see ResultRow
 * @since 1.0
 */
public interface ResultStates extends ResultItem {

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

    /**
     * @return <ul>
     * <li>true : this instance representing the terminator of query result (eg: SELECT command)</li>
     * <li>false : this instance representing the update result (eg: INSERT/UPDATE/DELETE command)</li>
     * </ul>
     */
    boolean hasColumn();

    long rowCount();

    default int getWarnings() {
        throw new UnsupportedOperationException();
    }


    <T> T valueOf(Option<T> option);

}
