package io.jdbd.mysql.session;

import io.jdbd.DatabaseSession;
import io.jdbd.mysql.stmt.QueryAttr;
import io.jdbd.mysql.stmt.StatementOption;
import io.jdbd.stmt.Statement;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * <p>
 * This interface is a implementation of {@link Statement} with MySQL client protocol.
 * </p>
 */
abstract class MySQLStatement implements Statement {

    final MySQLDatabaseSession session;

    final MySQLStatementOption statementOption = new MySQLStatementOption();

    MySQLStatement(MySQLDatabaseSession session) {
        this.session = session;
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
    public boolean supportPublisher() {
        return false;
    }

    @Override
    public boolean supportOutParameter() {
        return false;
    }

    @Override
    public final void setTimeout(final int seconds) {
        this.statementOption.timeoutSeconds = seconds;
    }

    @Override
    public boolean setFetchSize(int fetchSize) {
        return false;
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


    @Nullable
    final void prepareAttrGroup(final Map<String, QueryAttr> attrGroup) {
        final Map<String, QueryAttr> commonAttrGroup = this.statementOption.commonAttrGroup;
        if (commonAttrGroup == null) {
            this.statementOption.commonAttrGroup = attrGroup;
        } else {
            commonAttrGroup.putAll(attrGroup);
        }
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

    /**
     * @return true attrGroupList size error.
     */
    @Nullable
    final Throwable checkBatchAttrGroupListSize(final int batchCount) {
        final List<Map<String, QueryAttr>> attrGroupList = this.statementOption.attrGroupList;
        final Throwable error;
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

        Map<String, QueryAttr> commonAttrGroup;

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
