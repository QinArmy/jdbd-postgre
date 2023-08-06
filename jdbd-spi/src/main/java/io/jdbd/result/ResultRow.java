package io.jdbd.result;

/**
 * <p>
 * This interface representing one row of query result (eg: SELECT command).
 * </p>
 * <p>
 * The instance of this interface always after the instance of {@link ResultRowMeta} and before the instance of {@link ResultStates}
 * in the {@link OrderedFlux} of same query result.
 * </p>
 * <p>
 * The {@link #getResultNo()} of this interface always return same value with {@link ResultRowMeta} in same query result.
 * See {@link #getRowMeta()}
 * </p>
 *
 * @see ResultRowMeta
 * @see ResultStates
 * @since 1.0
 */
public interface ResultRow extends JdbdRow {


}
