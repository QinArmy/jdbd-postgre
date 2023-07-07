package io.jdbd.statement;

import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.OutResult;

/**
 * <p>
 * This interface representing out parameter of stored procedure.
 * You create instance of {@link OutParameter} by {@link OutParameter#from(String, Object)}.
 * </p>
 *
 * @see ParametrizedStatement#bind(int, DataType, Object)
 * @see io.jdbd.result.OutResult
 * @since 1.0
 */
public interface OutParameter {

    /**
     * @return out parameter name.
     * @see OutResult#outParamName()
     */
    String name();

    /**
     * @return out parameter value.
     */
    @Nullable
    Object value();

    /**
     * override {@link Object#hashCode()}
     */
    @Override
    int hashCode();

    /**
     * override {@link Object#equals(Object)}
     */
    @Override
    boolean equals(Object obj);

    /**
     * override {@link Object#toString()}
     *
     * @return out parameter info, contain {@link System#identityHashCode(Object)},
     * but if {@link #value()} is {@link String} , then always output {@code ?} ,because of information safe.
     */
    @Override
    String toString();


    static OutParameter from(String name, @Nullable Object value) {
        return JdbdOutParameter.create(name, value);
    }


}
