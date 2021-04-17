package io.jdbd.mysql.stmt;

import io.jdbd.MultiResults;
import io.jdbd.ResultStates;
import io.jdbd.vendor.stmt.JdbdStmtWrappers;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class StmtWrappers extends JdbdStmtWrappers {

    protected StmtWrappers() {
        throw new UnsupportedOperationException();
    }


    public static BindableWrapper single(String sql, BindValue bindValue) {
        return new SimpleBindableWrapper(sql, bindValue);
    }

    public static BindableWrapper multi(String sql, List<BindValue> paramGroup) {
        return new SimpleBindableWrapper(sql, paramGroup);
    }

    public static BatchBindWrapper batchBind(String sql, List<List<BindValue>> paramGroupList) {
        return new SimpleBatchBindWrapper(sql, paramGroupList);
    }


    private static final class SimpleBindableWrapper implements BindableWrapper {

        private final String sql;

        private final List<BindValue> paramGroup;

        private SimpleBindableWrapper(String sql, List<BindValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = Collections.unmodifiableList(paramGroup);
        }

        private SimpleBindableWrapper(String sql, BindValue bindValue) {
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
        public Consumer<ResultStates> getStatesConsumer() {
            return MultiResults.EMPTY_CONSUMER;
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


    private static final class SimpleBatchBindWrapper implements BatchBindWrapper {

        private final String sql;

        private final List<List<BindValue>> paramGroupList;

        private SimpleBatchBindWrapper(String sql, List<List<BindValue>> paramGroupList) {
            this.sql = sql;
            this.paramGroupList = Collections.unmodifiableList(paramGroupList);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<BindValue>> getParamGroupList() {
            return this.paramGroupList;
        }


        @Override
        public int getTimeout() {
            return 0;
        }


    }


}
