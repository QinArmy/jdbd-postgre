package io.jdbd.session;

import java.util.Map;

/**
 * <p>
 * This enum representing An XA transaction progresses states.
 * </p>
 *
 * @see Option#XA_STATES
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/xa-states.html">XA Transaction States</a>
 * @since 1.0
 */
public enum XaStates {

    /**
     * <p>
     * This instance representing XA transaction started after {@link RmDatabaseSession#start(Xid, int, TransactionOption)} method.
     * </p>
     */
    ACTIVE,

    /**
     * <p>
     * This instance representing XA transaction IDLE after {@link RmDatabaseSession#end(Xid, int, Map)} method.
     * </p>
     * <p>
     * This states support one-phase commit.
     * </p>
     */
    IDLE,

    /**
     * <p>
     * This instance representing XA transaction PREPARED after {@link RmDatabaseSession#prepare(Xid, Map)} method.
     * </p>
     */
    PREPARED;


    @Override
    public final String toString() {
        return String.format("%s.%s", XaStates.class.getName(), this.name());
    }


}
