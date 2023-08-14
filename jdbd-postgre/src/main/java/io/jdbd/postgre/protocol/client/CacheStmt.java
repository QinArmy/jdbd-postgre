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
interface CacheStmt {

    String originalSql();

    String postgreSql();

    boolean isStandardConformingStrings();

    int useCount();


}
