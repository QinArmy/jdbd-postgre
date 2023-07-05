package io.jdbd.meta;

import io.jdbd.statement.ParameterStatement;

import java.sql.JDBCType;

/**
 * <p>
 * This interface representing sql data type,this interface is used by following:
 *     <ul>
 *         <li>{@link ParameterStatement#bind(int, DataType, Object)}</li>
 *         <li>{@link io.jdbd.result.ResultRowMeta#getDataType(int)}</li>
 *     </ul>
 *     {@link ParameterStatement#bind(int, DataType, Object)} use {@link #typeName()} bind parameter.
 * </p>
 * <p>
 *     The Known superinterfaces:
 *     <ul>
 *         <li>{@link SQLType} representing database build-in data type</li>
 *     </ul>
 * </p>
 * <p>
 *     The Known implementations:
 *     <ul>
 *         <li>{@link SQLType} representing database build-in data type</li>
 *     </ul>
 * </p>
 *
 * @see SQLType
 * @since 1.0
 */
public interface DataType {

    /**
     * @return alias of data type in java language.
     */
    String name();

    JDBCType jdbcType();

    /**
     * @return data type name in database. If support ,upper case precedence.
     */
    String typeName();


    boolean isArray();

    boolean isUnknown();

    BooleanMode isUserDefined();

}
