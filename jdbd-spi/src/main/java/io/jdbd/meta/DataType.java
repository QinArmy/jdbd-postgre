package io.jdbd.meta;

import io.jdbd.statement.ParametrizedStatement;

/**
 * <p>
 * This interface representing sql data type,this interface is used by following:
 *     <ul>
 *         <li>{@link ParametrizedStatement#bind(int, DataType, Object)}</li>
 *         <li>{@link io.jdbd.result.ResultRowMeta#getDataType(int)}</li>
 *     </ul>
 *     {@link ParametrizedStatement#bind(int, DataType, Object)} use {@link #typeName()} bind parameter, if not {@link JdbdType}.
 * </p>
 * <p>
 *     The Known superinterfaces:
 *     <ul>
 *         <li>{@link SQLType} representing database build-in / internal-use data type</li>
 *     </ul>
 * </p>
 * <p>
 *     The Known implementations:
 *     <ul>
 *         <li>{@link JdbdType} generic sql type</li>
 *     </ul>
 * </p>
 *
 * @see SQLType
 * @see JdbdType
 * @since 1.0
 */
public interface DataType {

    /**
     * @return alias of data type in java language.
     */
    String name();

    /**
     * @return data type name in database. If support ,upper case precedence. If array end with [] .
     */
    String typeName();


    boolean isArray();

    boolean isUnknown();


    boolean isUserDefined();

}
