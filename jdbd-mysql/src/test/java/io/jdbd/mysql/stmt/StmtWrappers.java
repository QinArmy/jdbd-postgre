package io.jdbd.mysql.stmt;

import io.jdbd.MultiResults;
import io.jdbd.ResultStates;
import io.jdbd.mysql.BindValue;
import io.jdbd.mysql.StmtWrapper;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class StmtWrappers {

    protected StmtWrappers() {
        throw new UnsupportedOperationException();
    }

    public static StmtWrapper single(String sql, BindValue bindValue) {
        return new SingleStmtWrapper(sql, bindValue);
    }

    public static StmtWrapper multi(String sql, List<BindValue> bindValueList) {
        return new SingleStmtWrapper(sql, bindValueList);
    }


    private static final class SingleStmtWrapper implements StmtWrapper {

        private final String sql;

        private final List<BindValue> bindValueList;

        private SingleStmtWrapper(String sql, BindValue bindValue) {
            this.sql = sql;
            this.bindValueList = Collections.singletonList(bindValue);
        }

        private SingleStmtWrapper(String sql, List<BindValue> bindValueList) {
            this.sql = sql;
            this.bindValueList = Collections.unmodifiableList(bindValueList);
        }

        @Override
        public List<BindValue> getParameterGroup() {
            return this.bindValueList;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public Consumer<ResultStates> getStatesConsumer() {
            return MultiResults.EMPTY_CONSUMER;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }
    }


}
