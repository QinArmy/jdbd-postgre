package io.jdbd.mysql.session;

import io.jdbd.JdbdSQLException;
import io.jdbd.mysql.MySQLType;
import io.jdbd.mysql.stmt.AttrStatement;
import io.jdbd.mysql.stmt.QueryAttr;
import io.jdbd.mysql.stmt.StatementOption;
import io.jdbd.session.DatabaseSession;
import io.jdbd.stmt.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
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
    public final void bindCommonAttr(final String name, final MySQLType type, @Nullable Object value) {
        checkReuse();
        Objects.requireNonNull(name, "name");
        if (value instanceof Publisher || value instanceof Path) {
            String m = String.format("Query Attribute don't support java type[%s]", value.getClass().getName());
            throw new JdbdSQLException(new SQLException(m));
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
    public final void setTimeout(final int seconds) {
        this.statementOption.timeoutSeconds = seconds;
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

    abstract void checkReuse() throws JdbdSQLException;

    /**
     * <p>
     * for below methods:
     *     <ul>
     *         <li>executeUpdate()</li>
     *         <li>executeQuery()</li>
     *     </ul>
     * </p>
     */
    @Nullable
    final void prepareAttrGroup(final Map<String, QueryAttr> attrGroup) {
        final Map<String, QueryAttr> commonAttrGroup = this.statementOption.commonAttrGroup;
        if (commonAttrGroup == null) {
            this.statementOption.commonAttrGroup = attrGroup;
        } else {
            commonAttrGroup.putAll(attrGroup);
        }
    }

    final boolean attrGroupListNotEmpty() {
        final List<Map<String, QueryAttr>> attrGroupList = this.statementOption.attrGroupList;
        return attrGroupList != null && attrGroupList.size() > 0;
    }

    final void prepareAttrGroupList(final int batchCount) {
        List<Map<String, QueryAttr>> attrGroupList = this.statementOption.attrGroupList;
        if (batchCount > 0 && attrGroupList == null) {
            attrGroupList = new ArrayList<>();
            for (int i = 0; i < batchCount; i++) {
                attrGroupList.add(Collections.emptyMap());
            }
            this.statementOption.attrGroupList = attrGroupList;
        }
    }

    final void addBatchQueryAttr(@Nullable final Map<String, QueryAttr> attrGroup) {
        List<Map<String, QueryAttr>> attrGroupList = this.statementOption.attrGroupList;
        if (attrGroup == null) {
            if (attrGroupList != null) {
                attrGroupList.add(Collections.emptyMap());
            }
        } else {
            if (attrGroupList == null) {
                attrGroupList = new ArrayList<>();
                this.statementOption.attrGroupList = attrGroupList;
            }
            attrGroupList.add(attrGroup);
        }

    }

    /**
     * @return true attrGroupList size error.
     */
    @Nullable
    final IllegalStateException checkBatchAttrGroupListSize(final int batchCount) {
        final List<Map<String, QueryAttr>> attrGroupList = this.statementOption.attrGroupList;
        final IllegalStateException error;
        if (attrGroupList != null && attrGroupList.size() != batchCount) {
            // here bug
            String m = String.format("batch count[%s] and attrGroupList size[%s] not match."
                    , batchCount, attrGroupList.size());
            error = new IllegalStateException(m);
        } else {
            error = null;
        }
        return error;
    }


    static final class MySQLStatementOption implements StatementOption {

        private int timeoutSeconds;

        int fetchSize;

        private Map<String, QueryAttr> commonAttrGroup;

        List<Map<String, QueryAttr>> attrGroupList;

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
        public Map<String, QueryAttr> getCommonAttrs() {
            Map<String, QueryAttr> commonAttrGroup = this.commonAttrGroup;
            if (commonAttrGroup == null) {
                commonAttrGroup = Collections.emptyMap();
            } else {
                this.commonAttrGroup = null;
            }
            return commonAttrGroup;
        }

        @Override
        public List<Map<String, QueryAttr>> getAttrGroupList() {
            List<Map<String, QueryAttr>> attrGroupList = this.attrGroupList;

            if (attrGroupList == null) {
                attrGroupList = Collections.emptyList();
            } else {
                this.attrGroupList = null;
            }
            return attrGroupList;
        }

    }


}
