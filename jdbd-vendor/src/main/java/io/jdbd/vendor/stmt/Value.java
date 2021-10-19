package io.jdbd.vendor.stmt;

import reactor.util.annotation.Nullable;

/**
 * <p>
 * This interface representing a value that is bound to sql.
 * This is a base interface of below interface:
 * <p>
 * <UL>
 * <li>{@link ParamValue}</li>
 * <li>{@link NamedValue }</li>
 * <li>{@link TypeValue }</li>
 * </UL>
 * </p>
 * </p>
 *
 * @see ParamValue
 * @see NamedValue
 */
public interface Value {

    @Nullable
    Object get();

    Object getNonNull() throws NullPointerException;

}
