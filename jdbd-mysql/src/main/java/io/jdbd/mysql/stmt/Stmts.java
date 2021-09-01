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


    public static BindableStmt single(String sql, BindValue bindValue) {
        return new SimpleBindableStmt(sql, bindValue);
    }

    public static BindableStmt multi(String sql, List<BindValue> paramGroup) {
        return new SimpleBindableStmt(sql, paramGroup);
    }

    public static BatchBindStmt batchBind(String sql, List<List<BindValue>> paramGroupList) {
        return new SimpleBatchBindStmt(sql, paramGroupList);
    }


    private static final class SimpleBindableStmt implements BindableStmt {

        private final String sql;

        private final List<BindValue> paramGroup;

        private SimpleBindableStmt(String sql, List<BindValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = Collections.unmodifiableList(paramGroup);
        }

        private SimpleBindableStmt(String sql, BindValue bindValue) {
            this.sql = sql;
            this.paramGroup = Collections.singletonList(bindValue);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<BindValue> getParamGroup() {
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


    private static final class SimpleBatchBindStmt implements BatchBindStmt {

        private final String sql;

        private final List<List<BindValue>> groupList;

        private SimpleBatchBindStmt(String sql, List<List<BindValue>> groupList) {
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
