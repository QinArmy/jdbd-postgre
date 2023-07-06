package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.JdbdSQLException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.MySQLStmtOption;
import io.jdbd.mysql.stmt.QueryAttr;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.Statement;
import io.jdbd.vendor.stmt.NamedValue;
import io.jdbd.vendor.stmt.StmtOption;
import io.jdbd.vendor.util.JdbdExceptions;
import io.jdbd.vendor.util.JdbdStrings;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

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
abstract class MySQLStatement<S extends Statement> implements Statement, StmtOption {

    final MySQLDatabaseSession session;

    final MySQLStatementOption statementOption = new MySQLStatementOption();

    private int timeoutSeconds;

    private int fetchSize;


    MySQLStatement(MySQLDatabaseSession session) {
        this.session = session;
    }


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
    public final S bindStmtVar(final String name, final @Nullable DataType dataType,
                               final @Nullable Object nullable) throws JdbdException {
        RuntimeException error = null;
        if (!JdbdStrings.hasText(name)) {
            error = JdbdExceptions.stmtVarNameHaveNoText(name);
        } else if (dataType == null) {
            error = JdbdExceptions.dataTypeIsNull();
        }
        if (error != null) {
            this.closeOnBindError(error);
            throw JdbdExceptions.wrap(error);
        }
        return (S) this;
    }

    @Override
    public final DatabaseSession getSession() {
        return this.session;
    }

    @Override
    public final <T extends DatabaseSession> T getSession(final Class<T> sessionClass) {
        try {
            return sessionClass.cast(this.session);
        } catch (Throwable e) {
            closeOnBindError(e);
            throw JdbdExceptions.wrap(e);
        }
    }


    @Override
    public final boolean supportStmtVar() {
        return this.session.supportStmtVar();
    }

    @Override
    public final S setTimeout(int seconds) {
        this.timeoutSeconds = seconds;
        return (S) this;
    }

    @Override
    public final S setFetchSize(int fetchSize) throws JdbdException {
        this.fetchSize = fetchSize;
        return (S) this;
    }

    @Override
    public final S setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException {
        final JdbdException error;
        error = JdbdExceptions.dontSupportImporter(MySQLDatabaseSessionFactory.MY_SQL);
        this.closeOnBindError(error);
        throw error;
    }

    @Override
    public final S setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException {
        final JdbdException error;
        error = JdbdExceptions.dontSupportExporter(MySQLDatabaseSessionFactory.MY_SQL);
        this.closeOnBindError(error);
        throw error;
    }

    abstract void checkReuse() throws JdbdException;

    /**
     * @see MySQLPreparedStatement
     */
    void closeOnBindError(Throwable error) {
        // no-op
    }


    static final class MySQLStatementOption implements MySQLStmtOption {

        private Map<String, NamedValue> commonAttrMap;

        private Map<String, NamedValue> queryAttrMap;

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
        public Map<String, QueryAttr> getStmtVarMap() {
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
