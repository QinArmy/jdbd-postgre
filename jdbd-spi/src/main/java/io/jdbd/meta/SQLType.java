package io.jdbd.meta;

import io.jdbd.lang.Nullable;
import io.jdbd.result.ResultRow;

import java.sql.JDBCType;

/**
 * @since 1.0
 */
public interface SQLType extends DataType {


    /**
     * @see ResultRow#get(String)
     */
    Class<?> javaType();

    /**
     * <p>
     * For example:
     *    <ul>
     *        <li>one dimension BIGINT_ARRAY return BIGINT</li>
     *        <li>tow dimension BIGINT_ARRAY return BIGINT too</li>
     *    </ul>
     * </p>
     *
     * @return element type of array(1-n dimension)
     */
    @Nullable
    SQLType elementType();

    String getVendor();


    /**
     * @return tue : type is number and unsigned.
     */
    boolean isUnsigned();

    boolean isNumber();

    boolean isIntegerType();

    boolean isFloatType();

    boolean isLongString();

    boolean isLongBinary();

    boolean isStringType();

    boolean isBinaryType();

    boolean isTimeType();

    boolean isDecimal();

    boolean isCaseSensitive();

    boolean isArray();

    boolean supportPublisher();

    boolean supportTextPublisher();

    boolean supportBinaryPublisher();


}
