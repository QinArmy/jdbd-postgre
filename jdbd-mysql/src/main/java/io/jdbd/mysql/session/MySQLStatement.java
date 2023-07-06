package io.jdbd.mysql.session;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.util.MySQLBinds;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.session.DatabaseSession;
import io.jdbd.statement.Statement;
import io.jdbd.vendor.stmt.JdbdValues;
import io.jdbd.vendor.stmt.NamedValue;
import io.jdbd.vendor.stmt.StmtOption;
import io.jdbd.vendor.util.JdbdStrings;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


/**
 * <p>
 * This interface is a implementation of {@link Statement} with MySQL client protocol.
 * </p>
 */
abstract class MySQLStatement<S extends Statement> implements Statement, StmtOption {

    final MySQLDatabaseSession session;

    private int timeoutSeconds;

    int fetchSize;

    private Map<String, NamedValue> queryAttrMap;


    MySQLStatement(MySQLDatabaseSession session) {
        this.session = session;
    }


    @SuppressWarnings("unchecked")
    @Override
    public final S bindStmtVar(final String name, final @Nullable DataType dataType,
                               final @Nullable Object nullable) throws JdbdException {
        RuntimeException error = null;
        final MySQLType type;
        if (!JdbdStrings.hasText(name)) {
            error = MySQLExceptions.stmtVarNameHaveNoText(name);
        } else if (dataType == null) {
            error = MySQLExceptions.dataTypeIsNull();
        } else if (nullable instanceof Publisher || nullable instanceof Path) {
            error = MySQLExceptions.dontSupportJavaType(name, nullable, MySQLDatabaseSessionFactory.MY_SQL);
        } else if ((type = MySQLBinds.handleDataType(dataType)) == null) {
            error = MySQLExceptions.dontSupportDataType(dataType, MySQLDatabaseSessionFactory.MY_SQL);
        } else {
            Map<String, NamedValue> map = this.queryAttrMap;
            if (map == null) {
                this.queryAttrMap = map = MySQLCollections.hashMap();
            } else if (!(map instanceof HashMap)) {
                // here,have closed
                throw MySQLExceptions.cannotReuseStatement(getClass());
            }

            if (map.putIfAbsent(name, JdbdValues.namedValue(name, type, nullable)) != null) {
                error = MySQLExceptions.stmtVarDuplication(name);
            }
        }

        if (error != null) {
            this.closeOnBindError(error);
            throw MySQLExceptions.wrap(error);
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
            throw MySQLExceptions.wrap(e);
        }
    }


    @Override
    public final boolean supportStmtVar() {
        return this.session.supportStmtVar();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setTimeout(int seconds) {
        this.timeoutSeconds = seconds;
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final S setFetchSize(int fetchSize) throws JdbdException {
        this.fetchSize = fetchSize;
        return (S) this;
    }

    @Override
    public final S setImportPublisher(Function<Object, Publisher<byte[]>> function) throws JdbdException {
        final JdbdException error;
        error = MySQLExceptions.dontSupportImporter(MySQLDatabaseSessionFactory.MY_SQL);
        this.closeOnBindError(error);
        throw error;
    }

    @Override
    public final S setExportSubscriber(Function<Object, Subscriber<byte[]>> function) throws JdbdException {
        final JdbdException error;
        error = MySQLExceptions.dontSupportExporter(MySQLDatabaseSessionFactory.MY_SQL);
        this.closeOnBindError(error);
        throw error;
    }


    @Override
    public final int getTimeout() {
        return this.timeoutSeconds;
    }

    @Override
    public final int getFetchSize() {
        return this.fetchSize;
    }

    @Override
    public final Map<String, NamedValue> getStmtVarMap() {
        Map<String, NamedValue> map = this.queryAttrMap;
        if (map == null) {
            map = Collections.emptyMap();
        } else if (map instanceof HashMap) {
            map = MySQLCollections.unmodifiableMap(map);
        }// here couldn't modify this.queryAttrMap
        return map;
    }

    @Override
    public final Function<Object, Publisher<byte[]>> getImportPublisher() {
        // always null
        return null;
    }

    @Override
    public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
        // always null
        return null;
    }

    final void endStmtOption() {
        final Map<String, NamedValue> map = this.queryAttrMap;
        if (map == null) {
            this.queryAttrMap = Collections.emptyMap();
        } else if (map instanceof HashMap) {
            this.queryAttrMap = MySQLCollections.unmodifiableMap(map);
        }
    }

    abstract void checkReuse() throws JdbdException;

    /**
     * @see MySQLPreparedStatement
     */
    void closeOnBindError(Throwable error) {
        // no-op
    }


}
