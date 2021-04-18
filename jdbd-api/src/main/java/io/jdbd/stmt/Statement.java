package io.jdbd.stmt;


/**
 * <p>
 * This interface is base interface of below:
 *     <ul>
 *         <li>{@link StaticStatement}</li>
 *         <li>{@link BindableStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 *
 * @see StaticStatement
 * @see BindableStatement
 * @see PreparedStatement
 * @see MultiStatement
 */
public interface Statement {

    boolean supportLongData();

}
