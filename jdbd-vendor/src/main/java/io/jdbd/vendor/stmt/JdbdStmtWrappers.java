package io.jdbd.vendor.stmt;


import io.jdbd.result.MultiResults;
import io.jdbd.result.ResultStates;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class JdbdStmtWrappers {

    protected JdbdStmtWrappers() {
        throw new UnsupportedOperationException();
    }


    public static BatchWrapper<ParamValue> batch(String sql, List<List<ParamValue>> groupList) {
        if (groupList.size() < 2) {
            throw new IllegalArgumentException("groupList size < 2");
        }
        return new BatchWrapperImpl(sql, groupList, 0);
    }

    public static BatchWrapper<ParamValue> batch(String sql, List<List<ParamValue>> groupList, int timeOut) {
        if (groupList.size() < 2) {
            throw new IllegalArgumentException("groupList size < 2");
        }
        return new BatchWrapperImpl(sql, groupList, timeOut);
    }

    public static ParamWrapper singlePrepare(String sql, ParamValue paramValue) {
        return new SimpleParamWrapper(sql, paramValue);
    }

    public static ParamWrapper multiPrepare(String sql, List<? extends ParamValue> paramGroup) {
        return new SimpleParamWrapper(sql, paramGroup);
    }


    private static final class BatchWrapperImpl implements BatchWrapper<ParamValue> {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private final int timeOut;

        private BatchWrapperImpl(String sql, List<List<ParamValue>> groupList, int timeOut) {
            this.sql = sql;
            this.groupList = Collections.unmodifiableList(groupList);
            this.timeOut = timeOut;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final List<List<ParamValue>> getParamGroupList() {
            return this.groupList;
        }

        @Override
        public final int getTimeout() {
            return this.timeOut;
        }


    }


    private static final class SimpleParamWrapper implements ParamWrapper {

        private final String sql;

        private final List<? extends ParamValue> paramGroup;

        private SimpleParamWrapper(String sql, ParamValue paramValue) {
            this.sql = sql;
            this.paramGroup = Collections.singletonList(paramValue);
        }

        private SimpleParamWrapper(String sql, List<? extends ParamValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = Collections.unmodifiableList(paramGroup);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<? extends ParamValue> getParamGroup() {
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

    protected static final class ParamWrapperImpl implements ParamWrapper {

        @Override
        public String getSql() {
            return null;
        }

        @Override
        public List<? extends ParamValue> getParamGroup() {
            return null;
        }

        @Override
        public Consumer<ResultStates> getStatesConsumer() {
            return null;
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


}
