package io.jdbd.meta;

import io.jdbd.lang.Nullable;

/**
 * @since 1.0
 */
public interface SQLType extends DataType {

    JdbdType jdbdType();


    Class<?> firstJavaType();

    @Nullable
    Class<?> secondJavaType();


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


}
