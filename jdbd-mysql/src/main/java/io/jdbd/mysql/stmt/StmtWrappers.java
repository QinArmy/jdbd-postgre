package io.jdbd.mysql.stmt;

import io.jdbd.result.MultiResult;
import io.jdbd.result.ResultStatus;
import io.jdbd.vendor.stmt.JdbdStmtWrappers;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class StmtWrappers extends JdbdStmtWrappers {

    protected StmtWrappers() {
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
        public Consumer<ResultStatus> getStatusConsumer() {
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

        private final List<List<BindValue>> paramGroupList;

        private SimpleBatchBindStmt(String sql, List<List<BindValue>> paramGroupList) {
            this.sql = sql;
            this.paramGroupList = Collections.unmodifiableList(paramGroupList);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<BindValue>> getGroupList() {
            return this.paramGroupList;
        }


        @Override
        public int getTimeout() {
            return 0;
        }


    }


}
