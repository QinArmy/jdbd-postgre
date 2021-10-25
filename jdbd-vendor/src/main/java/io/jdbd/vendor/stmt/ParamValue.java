package io.jdbd.vendor.stmt;

/**
 * <p>
 * This interface extends {@link Value} ,representing a named value that is bound to sql.
 * This interface can be used by the implementation jdbd spi for below:
 * <ul>
 *     <li>{@link io.jdbd.stmt.PreparedStatement}</li>
 *     <li>{@link io.jdbd.stmt.BindStatement}</li>
 *     <li>{@link io.jdbd.stmt.MultiStatement}</li>
 * </ul>
 * </p>
 */
public interface ParamValue extends Value {

    int getIndex();

    boolean isLongData();

}
