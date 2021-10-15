package io.jdbd.mysql.stmt;

import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.JdbdStmts;
import io.jdbd.vendor.stmt.ParamBatchStmt;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.util.JdbdFunctions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class Stmts extends JdbdStmts {

    protected Stmts() {
        throw new UnsupportedOperationException();
    }


    public static ParamStmt paramStmt(String sql, List<ParamValue> bindGroup, StatementOption option) {
        return new FullUpdateMySQLParamStmt(sql, bindGroup, option);
    }

    public static ParamStmt paramStmt(String sql, List<ParamValue> paramGroup
            , Consumer<ResultStates> statesConsumer, StatementOption option) {
        return new FullQueryMySQLParamStmt(sql, paramGroup, statesConsumer, option);
    }

    public static ParamBatchStmt<ParamValue> paramBatch(String sql, List<List<ParamValue>> groupList
            , StatementOption option) {
        return new FullMySQLParamBatchStmt(sql, groupList, option);
    }


    public static BindStmt single(String sql, BindValue bindValue) {
        return new SimpleBindStmt(sql, bindValue);
    }

    public static BindStmt multi(String sql, List<BindValue> paramGroup) {
        return new SimpleBindStmt(sql, paramGroup);
    }

    public static BindBatchStmt batchBind(String sql, List<List<BindValue>> paramGroupList) {
        return new SimpleBindBatchStmt(sql, paramGroupList);
    }


    private static final class SimpleBindStmt extends AbstractMinStmt implements BindStmt {

        private final String sql;

        private final List<BindValue> paramGroup;

        private SimpleBindStmt(String sql, List<BindValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = Collections.unmodifiableList(paramGroup);
        }

        private SimpleBindStmt(String sql, BindValue bindValue) {
            this.sql = sql;
            this.paramGroup = Collections.singletonList(bindValue);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<BindValue> getBindGroup() {
            return this.paramGroup;
        }


        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return MultiResult.EMPTY_CONSUMER;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

    }


    private static final class SimpleBindBatchStmt extends AbstractMinStmt implements BindBatchStmt {

        private final String sql;

        private final List<List<BindValue>> groupList;

        private SimpleBindBatchStmt(String sql, List<List<BindValue>> groupList) {
            this.sql = sql;
            if (groupList.size() == 1) {
                this.groupList = Collections.singletonList(groupList.get(0));
            } else {
                this.groupList = Collections.unmodifiableList(groupList);
            }

        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<BindValue>> getGroupList() {
            return this.groupList;
        }


        @Override
        public int getTimeout() {
            return 0;
        }


    }

    private static class FullUpdateMySQLParamStmt implements ParamStmt, MySQLParamSingleStmt {

        private final String sql;

        private final List<ParamValue> bindGroup;

        private final int timeout;

        private final Map<String, QueryAttr> queryAttrs;


        private FullUpdateMySQLParamStmt(final String sql, final List<ParamValue> bindGroup, final StatementOption option) {
            this.sql = sql;
            this.bindGroup = MySQLCollections.unmodifiableList(bindGroup);
            this.timeout = option.getTimeout();

            final Map<String, QueryAttr> queryAttrMap = option.getCommonAttrs();
            if (queryAttrMap.isEmpty()) {
                this.queryAttrs = queryAttrMap;
            } else {
                this.queryAttrs = Collections.unmodifiableMap(queryAttrMap);
            }
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return JdbdFunctions.noActionConsumer();
        }

        @Override
        public final List<? extends ParamValue> getBindGroup() {
            return this.bindGroup;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public final Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public final Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }

        @Override
        public final Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
        }


    }

    private static final class FullQueryMySQLParamStmt extends FullUpdateMySQLParamStmt {

        private final int fetchSize;

        private final Consumer<ResultStates> statesConsumer;

        private FullQueryMySQLParamStmt(String sql, List<ParamValue> bindGroup
                , Consumer<ResultStates> statesConsumer, StatementOption option) {
            super(sql, bindGroup, option);
            this.fetchSize = option.getFetchSize();
            this.statesConsumer = statesConsumer;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

        @Override
        public int getFetchSize() {
            return this.fetchSize;
        }
    }

    private static final class FullMySQLParamBatchStmt implements MySQLParamBatchStmt<ParamValue> {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private final int timeout;

        private final int fetchSize;

        private final Map<String, QueryAttr> queryAttrs;

        private final List<Map<String, QueryAttr>> attrGroupList;


        private FullMySQLParamBatchStmt(final String sql, final List<List<ParamValue>> groupList, final StatementOption option) {
            this.sql = sql;
            this.groupList = MySQLCollections.unmodifiableList(groupList);
            this.timeout = option.getTimeout();

            this.fetchSize = groupList.size() == 1 ? option.getFetchSize() : 0;

            final Map<String, QueryAttr> queryAttrMap = option.getCommonAttrs();
            if (queryAttrMap.size() == 0) {
                this.queryAttrs = queryAttrMap;
            } else {
                this.queryAttrs = Collections.unmodifiableMap(queryAttrMap);
            }
            this.attrGroupList = MySQLCollections.unmodifiableList(option.getAttrGroupList());

        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<ParamValue>> getGroupList() {
            return this.groupList;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }


        @Override
        public Map<String, QueryAttr> getQueryAttrs() {
            return this.queryAttrs;
        }

        @Override
        public List<Map<String, QueryAttr>> getQueryAttrGroup() {
            return this.attrGroupList;
        }

        @Override
        public int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportPublisher() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportSubscriber() {
            return null;
        }


    }


}
