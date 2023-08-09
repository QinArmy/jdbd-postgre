package io.jdbd.postgre.session;

import io.jdbd.JdbdException;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.session.DatabaseMetaSpec;
import io.jdbd.session.Option;
import io.jdbd.session.ServerVersion;

/**
 * <p>
 * This class is base class of following :
 *     <ul>
 *         <li>{@link PgDatabaseSession}</li>
 *         <li>{@link PgDatabaseMetaData}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
abstract class PgDatabaseMetaSpec implements DatabaseMetaSpec {

    final PgProtocol protocol;


    PgDatabaseMetaSpec(PgProtocol protocol) {
        this.protocol = protocol;
    }


    @Override
    public final ServerVersion serverVersion() throws JdbdException {
        return this.protocol.serverVersion();
    }

    @Override
    public final boolean isSupportSavePoints() throws JdbdException {
        //always true, postgre support save points
        return true;
    }

    @Override
    public final boolean isSupportStmtVar() throws JdbdException {
        //always false, postgre don't support statement-variable
        return false;
    }

    @Override
    public final boolean isSupportMultiStatement() throws JdbdException {
        //always true, postgre support multi-statement
        return true;
    }

    @Override
    public final boolean isSupportOutParameter() throws JdbdException {
        //always true, postgre support out parameter.
        return true;
    }

    @Override
    public final boolean isSupportStoredProcedures() throws JdbdException {
        //always true, postgre support procedures
        return true;
    }

    @Override
    public final boolean isSupportRefCursor() throws JdbdException {
        //always true, postgre support declare cursor.
        return true;
    }

    @Override
    public final boolean iSupportLocalTransaction() {
        return true;
    }

    @Override
    public final <T> T valueOf(Option<T> option) {
        return this.protocol.valueOf(option);
    }


}
