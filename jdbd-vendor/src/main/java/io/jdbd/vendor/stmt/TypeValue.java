package io.jdbd.vendor.stmt;


/**
 * <p>
 * This interface extends {@link Value},representing a value that is bound to sql
 * and can wrap {@link io.jdbd.meta.SQLType} .
 * This interface can be used by the implementation jdbd spi for below:
 * <ul>
 *     <li>{@link io.jdbd.statement.BindStatement}</li>
 *     <li>{@link io.jdbd.statement.MultiStatement}</li>
 * </ul>
 * </p>
 */
@Deprecated
public interface TypeValue extends Value {


}
