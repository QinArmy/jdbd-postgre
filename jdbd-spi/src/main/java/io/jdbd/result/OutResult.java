package io.jdbd.result;

import io.jdbd.statement.OutParameter;

/**
 * <p>
 * This interface representing out parameter of stored procedure, that is returned by database server.
 * </p>
 *
 * @see OutParameter
 * @since 1.0
 */
public interface OutResult extends ResultItem {

    /**
     * @return out parameter name of stored procedure
     * @see OutParameter#name()
     */
    String outParamName();


}
