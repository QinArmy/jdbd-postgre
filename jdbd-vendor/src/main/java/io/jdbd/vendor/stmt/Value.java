package io.jdbd.vendor.stmt;


import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.statement.Parameter;

/**
 * <p>
 * This interface representing a value that is bound to sql.
 * This is a base interface of below interface:
 * <p>
 * <UL>
 * <li>{@link ParamValue}</li>
 * <li>{@link NamedValue }</li>
 * </UL>
 * </p>
 * </p>
 *
 * @see ParamValue
 * @see NamedValue
 */
public interface Value {

    /**
     * @return parameter value that maybe is {@link Parameter}
     */
    @Nullable
    Object get();

    /**
     * @return non-null parameter that maybe is {@link Parameter}
     * @throws NullPointerException throw when parameter is null
     */
    Object getNonNull() throws NullPointerException;

    /**
     * @return parameter value or {@link Parameter#value()}
     */
    @Nullable
    Object getValue();

    /**
     * @return non-null parameter value or {@link Parameter#value()}
     * @throws NullPointerException throw when parameter value is null or {@link Parameter#value()} is null.
     */
    Object getNonNullValue() throws NullPointerException;


    /**
     * @return sql type of bind value.
     */
    DataType getType();

}
