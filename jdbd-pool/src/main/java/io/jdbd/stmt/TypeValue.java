package io.jdbd.stmt;


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
public interface TypeValue extends Value {

    /**
     * <p>
     * This method will be override for return the implementation of {@link io.jdbd.meta.SQLType}.
     * </p>
     *
     * @return sql type of bind value.
     */
    io.jdbd.meta.SQLType getType();

}
