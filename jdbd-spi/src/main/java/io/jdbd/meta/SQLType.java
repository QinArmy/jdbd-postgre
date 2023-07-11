package io.jdbd.meta;

import io.jdbd.lang.Nullable;

/**
 * @since 1.0
 */
public interface SQLType extends DataType {

    JdbdType jdbdType();


    /**
     * @see io.jdbd.result.JdbdRow#get(String)
     */
    Class<?> outputJavaType();

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

    String vendor();


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

    boolean supportPublisher();

    boolean supportTextPublisher();

    boolean supportBinaryPublisher();


}
