package io.jdbd.mysql.stmt;

import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.JdbdStmts;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class Stmts extends JdbdStmts {

    protected Stmts() {
        throw new UnsupportedOperationException();
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


    private static final class SimpleBindStmt implements BindStmt {

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
        public int getFetchSize() {
            return 0;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

    }


    private static final class SimpleBindBatchStmt implements BindBatchStmt {

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


}
