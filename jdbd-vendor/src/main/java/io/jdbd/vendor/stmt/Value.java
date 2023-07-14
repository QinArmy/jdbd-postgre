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
     * @return <ul>
     * <li>null</li>
     * <li>non-{@link Parameter} instance</li>
     * <li>{@link Parameter instance}</li>
     * </ul>
     */
    @Nullable
    Object get();

    /**
     * @return <ul>
     * <li>non-{@link Parameter} instance</li>
     * <li>{@link Parameter instance}</li>
     * </ul>
     * @throws NullPointerException throw when param is null
     */
    Object getNonNull() throws NullPointerException;

    /**
     * @return <ul>
     * <li>null</li>
     * <li>non-{@link Parameter} instance</li>
     * </ul>
     */
    @Nullable
    Object getValue();

    /**
     * @return non-{@link Parameter} instance
     * @throws NullPointerException throw when <ul>
     *                              <li>param is null</li>
     *                              <li>{@link Parameter#value()} is null</li>
     *                              </ul>
     */
    Object getNonNullValue() throws NullPointerException;

    /**
     * @return true : if {@link #getValue()} is {@link org.reactivestreams.Publisher} or {@link java.nio.file.Path}.
     */
    boolean isLongData();


    /**
     * @return sql type of bind value.
     */
    DataType getType();

}
