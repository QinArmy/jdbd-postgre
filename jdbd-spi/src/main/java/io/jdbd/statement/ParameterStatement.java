package io.jdbd.statement;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;

import java.sql.JDBCType;

/**
 * <p>
 *     This interface is base interface of following :
 *     <ul>
 *         <li>{@link BindStatement}</li>
 *         <li>{@link PreparedStatement}</li>
 *         <li>{@link OneStepPrepareStatement}</li>
 *         <li>{@link MultiStatement}</li>
 *     </ul>
 * </p>
 * @since 1.0
 */
public interface ParameterStatement extends Statement {


    /**
     * @see BindStatement#bind(int, Object)
     * @see PreparedStatement#bind(int, Object)
     */
    ParameterStatement bind(int indexBasedZero, @Nullable Object nullable) throws JdbdException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param jdbcType       mapping {@link JDBCType}
     * @param nullable       nullable null the parameter value
     */
    ParameterStatement bind(int indexBasedZero, JDBCType jdbcType, @Nullable Object nullable) throws JdbdException;

    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     * <p>
     * This method use {@link DataType#typeName()} bind parameter.
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullable       nullable the parameter value
     * @param dataType       nonNullValue mapping sql data type name(must upper case).
     */
    ParameterStatement bind(int indexBasedZero, DataType dataType, @Nullable Object nullable) throws JdbdException;


    /**
     * <p>
     * SQL parameter placeholder must be {@code ?}
     * </p>
     *
     * @param indexBasedZero parameter placeholder index based zero.
     * @param nullable       nullable the parameter value
     * @param dataTypeName        nonNullValue mapping sql data type name(must upper case).
     */
    ParameterStatement bind(int indexBasedZero, String dataTypeName, @Nullable Object nullable) throws JdbdException;

}
