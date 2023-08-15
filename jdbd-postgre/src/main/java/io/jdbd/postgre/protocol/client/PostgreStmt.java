package io.jdbd.postgre.protocol.client;

/**
 * <p>
 * This interface representing extended query statement.
 * </p>
 * <p>
 * This interface is base interface of {@link ServerCacheStmt}.
 * </p>
 *
 * @since 1.0
 */
interface PostgreStmt {

    /**
     * @return the sql that parameter placeholder is {@code ?} .
     */
    String originalSql();

    /**
     * @return the sql that parameter placeholder is {@code $n} .
     */
    String postgreSql();

    boolean isStandardConformingStrings();

    int useCount();

}
