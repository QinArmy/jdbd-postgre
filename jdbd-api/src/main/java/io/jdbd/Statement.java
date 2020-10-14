package io.jdbd;

/**
 * <p>
 *     This interface is reactive version of {@link java.sql.Statement}
 * </p>
 */
public interface Statement  extends GenericStatement{


    void addBatch(String sql);




}
