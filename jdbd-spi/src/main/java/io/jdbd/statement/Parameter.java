package io.jdbd.statement;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.type.*;

/**
 * <p>
 * This interface representing some special parameter.
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link InOutParameter}</li>
 *         <li>{@link Blob}</li>
 *         <li>{@link Clob}</li>
 *         <li>{@link Text}</li>
 *         <li>{@link BlobPath}</li>
 *         <li>{@link TextPath}</li>
 *     </ul>
 * </p>
 *
 * @see ParametrizedStatement#bind(int, DataType, Object)
 * @since 1.0
 */
public interface Parameter {

    /**
     * @return parameter value.
     */
    @Nullable
    Object value();


}
