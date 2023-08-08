package io.jdbd.result;

import io.jdbd.session.OptionSpec;

/**
 * <p>
 * This interface representing one item of result (update/query result).
 * <ul>
 *     <li>The update result always is represented by just one {@link ResultStates} instance.</li>
 *     <li>The query result is represented by following sequence :
 *         <ol>
 *             <li>one {@link ResultRowMeta} instance.</li>
 *             <li>0-N {@link ResultRow} instance.</li>
 *             <li>one {@link ResultStates} instance.</li>
 *         </ol>
 *         in  {@link OrderedFlux}.
 *     </li>
 * </ul>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link ResultRowMeta}</li>
 *         <li>{@link ResultRow}</li>
 *         <li>{@link ResultStates}</li>
 *         <li>{@link CurrentRow}</li>
 *     </ul>
 * </p>
 *
 * @see ResultRowMeta
 * @see ResultRow
 * @see ResultStates
 * @see CurrentRow
 * @since 1.0
 */
public interface ResultItem extends OptionSpec {


    /**
     * @return the number of this Query/Update result, based one. The first value is 1 .
     */
    int getResultNo();

}
