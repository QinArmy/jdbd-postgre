package io.jdbd.session;

import io.jdbd.JdbdException;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link DatabaseSession}</li>
 *         <li>{@link io.jdbd.meta.DatabaseMetaData}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface SessionMetaSpec extends OptionSpec {


    /**
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    boolean isSupportSavePoints() throws JdbdException;

    /**
     * @throws JdbdException throw if session have closed
     */
    boolean isSupportStmtVar() throws JdbdException;

    /**
     * @throws JdbdException throw if session have closed
     */
    boolean isSupportMultiStatement() throws JdbdException;

    /**
     * @throws JdbdException throw if session have closed
     */
    boolean isSupportOutParameter() throws JdbdException;

    /**
     * @throws JdbdException throw if session have closed
     */
    boolean isSupportStoredProcedures() throws JdbdException;

    /**
     * @return true : support {@link io.jdbd.result.RefCursor}
     * @throws JdbdException throw if session have closed
     */
    boolean isSupportRefCursor() throws JdbdException;


}
