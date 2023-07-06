package io.jdbd.mysql.session;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrStatement;
import io.jdbd.mysql.stmt.MySQLStmtOption;
import io.jdbd.mysql.stmt.QueryAttr;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;


/**
 * <p>
 * This interface is a implementation of {@link Statement} with MySQL client protocol.
 * </p>
 */
abstract class MySQLStatement implements Statement, AttrStatement {

    final MySQLDatabaseSession session;

    final MySQLStatementOption statementOption = new MySQLStatementOption();

    MySQLStatement(MySQLDatabaseSession session) {
        this.session = session;
    }


    @Override
    public final void bindQueryAttr(final String name, final MySQLType type, @Nullable Object value) {
        checkReuse();
        Objects.requireNonNull(name, "name");
        if (value instanceof Publisher || value instanceof Path) {

            String m = String.format("Query Attribute don't support java type[%s]", value.getClass().getName());
            JdbdSQLException error = new JdbdSQLException(new SQLException(m));
            closeOnBindError(error);
            throw error;
        }
        Map<String, QueryAttr> commonAttrMap = this.statementOption.commonAttrGroup;
        if (commonAttrMap == null) {
            commonAttrMap = new HashMap<>();
            this.statementOption.commonAttrGroup = commonAttrMap;
        }
        commonAttrMap.put(name, QueryAttr.wrap(name, type, value));
    }


    @Override
    public final DatabaseSession getSession() {
        return this.session;
    }

    @Override
    public final <T extends DatabaseSession> T getSession(Class<T> sessionClass) {
        return sessionClass.cast(this.session);
    }


    @Override
    public final Statement setTimeout(final int seconds) {
        this.statementOption.timeoutSeconds = seconds;
        return this;
    }


    @Override
    public final boolean setImportPublisher(Function<Object, Publisher<byte[]>> function) {
        //always false ,MySQL not support import.
        return false;
    }

    @Override
    public final boolean setExportSubscriber(Function<Object, Subscriber<byte[]>> function) {
        //always false ,MySQL not support export.
        return false;
    }


    @Override
    public final boolean supportStmtVar() {
        return this.session.supportStmtVar();
    }

    abstract void checkReuse() throws JdbdSQLException;

    /**
     * @see MySQLPreparedStatement
     */
    void closeOnBindError(Throwable error) {
        // no-op
    }


    static final class MySQLStatementOption implements MySQLStmtOption {

        private int timeoutSeconds;

        int fetchSize;

        private Map<String, QueryAttr> commonAttrGroup;

        private Map<String, QueryAttr> queryAttrGroup;

        @Override
        public int getTimeout() {
            return this.timeoutSeconds;
        }

        @Override
        public int getFetchSize() {
            final int fetchSize = this.fetchSize;
            if (fetchSize > 0) {
                this.fetchSize = 0;
            }
            return fetchSize;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

        @Override
        public Map<String, QueryAttr> getAttrGroup() {
            Map<String, QueryAttr> queryAttrGroup = this.queryAttrGroup;
            if (queryAttrGroup == null) {
                queryAttrGroup = Collections.emptyMap();
            } else {
                this.queryAttrGroup = null;
            }
            return queryAttrGroup;
        }


    }


}
