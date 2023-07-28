package io.jdbd.vendor.stmt;


import io.jdbd.lang.Nullable;
import io.jdbd.meta.DataType;
import io.jdbd.result.ResultStates;
import io.jdbd.session.ChunkOption;
import io.jdbd.vendor.util.JdbdCollections;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class JdbdStmts {

    protected JdbdStmts() {
        throw new UnsupportedOperationException();
    }

    public static final Consumer<ResultStates> IGNORE_RESULT_STATES = JdbdStmts::ignoreResultStates;

    protected static final List<ParamValue> EMPTY_PARAM_GROUP = Collections.emptyList();


    public static StaticStmt stmt(final String sql) {
        return new MinStaticStmt(sql);
    }

    public static StaticStmt stmt(final String sql, Consumer<ResultStates> statusConsumer) {
        if (statusConsumer == IGNORE_RESULT_STATES) {
            return new MinStaticStmt(sql);
        }
        return new MinQueryStaticStmt(sql, statusConsumer);
    }

    public static StaticBatchStmt batch(List<String> sqlGroup) {
        return new MinStaticBatchStmt(sqlGroup);
    }

    public static StaticStmt stmt(String sql, StmtOption option) {
        return new MinOptionStaticStmt(sql, option);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultStates> statesConsumer, StmtOption option) {
        if (statesConsumer == IGNORE_RESULT_STATES) {
            return new MinOptionStaticStmt(sql, option);
        }
        return new OptionQueryStaticStmt(sql, statesConsumer, option);
    }

    public static StaticBatchStmt batch(List<String> sqlGroup, StmtOption option) {
        return new OptionStaticBatchStmt(sqlGroup, option);
    }


    public static ParamStmt single(String sql, DataType type, @Nullable Object value) {
        return new MinParamStmt(sql, Collections.singletonList(JdbdValues.paramValue(0, type, value)));
    }

    public static ParamStmt paramStmt(final String sql, @Nullable List<ParamValue> paramGroup) {
        if (paramGroup == null) {
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            paramGroup = JdbdCollections.unmodifiableList(paramGroup);
        }
        return new MinParamStmt(sql, paramGroup);
    }

    public static ParamStmt paramStmt(String sql, @Nullable List<ParamValue> paramGroup,
                                      Consumer<ResultStates> statesConsumer) {
        if (paramGroup == null) {
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            paramGroup = JdbdCollections.unmodifiableList(paramGroup);
        }
        if (statesConsumer == IGNORE_RESULT_STATES) {
            return new MinParamStmt(sql, paramGroup);
        }
        return new QueryParamStmt(sql, paramGroup, statesConsumer);
    }

    public static ParamStmt paramStmt(final String sql, @Nullable List<ParamValue> paramGroup, StmtOption option) {
        if (paramGroup == null) {
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            paramGroup = JdbdCollections.unmodifiableList(paramGroup);
        }
        return new SimpleOptionParamStmt(sql, paramGroup, option);
    }

    public static ParamStmt paramStmt(String sql, @Nullable List<ParamValue> paramGroup,
                                      Consumer<ResultStates> statesConsumer, StmtOption option) {
        if (paramGroup == null) {
            paramGroup = EMPTY_PARAM_GROUP;
        } else {
            paramGroup = JdbdCollections.unmodifiableList(paramGroup);
        }
        if (statesConsumer == IGNORE_RESULT_STATES) {
            return new SimpleOptionParamStmt(sql, paramGroup, option);
        }
        return new QueryOptionParamStmt(sql, paramGroup, statesConsumer, option);
    }

    public static ParamBatchStmt paramBatch(String sql, List<List<ParamValue>> groupList) {
        return new MinParamBatchStmt(sql, groupList);
    }


    public static ParamBatchStmt paramBatch(String sql, List<List<ParamValue>> groupList, StmtOption option) {
        return new OptionParamBatchStmt(sql, groupList, option);
    }

    public static StaticMultiStmt multiStmt(String multiStmt) {
        return new MinStaticMultiStmt(multiStmt);
    }

    public static StaticMultiStmt multiStmt(String multiStmt, StmtOption option) {
        return new OptionStaticMultiStmt(multiStmt, option);
    }


    public static ParamMultiStmt paramMultiStmt(List<ParamStmt> stmtList, StmtOption option) {
        return new OptionMultiStmt(stmtList, option);
    }


    private static abstract class StmtWithoutOption implements Stmt {

        @Override
        public final int getTimeout() {
            return 0;
        }

        @Override
        public final int getFetchSize() {
            return 0;
        }

        @Override
        public final List<NamedValue> getStmtVarList() {
            return Collections.emptyList();
        }

        @Override
        public final Function<ChunkOption, Publisher<byte[]>> getImportFunction() {
            return null;
        }

        @Override
        public final Function<ChunkOption, Subscriber<byte[]>> getExportFunction() {
            return null;
        }

    }//StmtWithoutOption


    private static abstract class SingleStmtWithoutOption extends StmtWithoutOption implements SingleStmt {

        private final String sql;

        private SingleStmtWithoutOption(String sql) {
            this.sql = sql;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }


    }// SingleStmtWithoutOption

    private static final class MinStaticStmt extends SingleStmtWithoutOption implements StaticStmt {

        private MinStaticStmt(String sql) {
            super(sql);
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
        }

    }//MinStaticStmt

    private static final class MinQueryStaticStmt extends SingleStmtWithoutOption implements StaticStmt {

        private final Consumer<ResultStates> statesConsumer;

        private MinQueryStaticStmt(String sql, Consumer<ResultStates> statesConsumer) {
            super(sql);
            this.statesConsumer = statesConsumer;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }


    }//MinQueryStaticStmt


    private static final class MinStaticBatchStmt extends StmtWithoutOption implements StaticBatchStmt {

        private final List<String> sqlGroup;

        private MinStaticBatchStmt(final List<String> sqlGroup) {
            this.sqlGroup = Collections.unmodifiableList(sqlGroup);
        }

        @Override
        public List<String> getSqlGroup() {
            return this.sqlGroup;
        }

    }//SimpleStaticBatchStmt


    private static abstract class StmtWithOption implements Stmt {

        private final int timeout;

        private final int fetchSize;

        private final List<NamedValue> stmtVarList;

        private final Function<ChunkOption, Publisher<byte[]>> importFunc;

        private final Function<ChunkOption, Subscriber<byte[]>> exportFunc;

        private StmtWithOption(StmtOption option) {
            this.timeout = option.getTimeout();
            this.fetchSize = option.getFetchSize();
            this.stmtVarList = option.getStmtVarList();
            this.importFunc = option.getImportFunction();
            this.exportFunc = option.getExportFunction();
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

        @Override
        public final int getFetchSize() {
            return this.fetchSize;
        }

        @Override
        public final List<NamedValue> getStmtVarList() {
            return this.stmtVarList;
        }

        @Override
        public final Function<ChunkOption, Publisher<byte[]>> getImportFunction() {
            return this.importFunc;
        }

        @Override
        public final Function<ChunkOption, Subscriber<byte[]>> getExportFunction() {
            return this.exportFunc;
        }


    }//StmtWithOption


    private static abstract class SingleStmtWithOption extends StmtWithOption implements SingleStmt {

        private final String sql;

        private SingleStmtWithOption(String sql, StmtOption option) {
            super(option);
            this.sql = sql;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }


    }//SingleStmtWithOption


    private static final class MinOptionStaticStmt extends SingleStmtWithOption implements StaticStmt {

        private MinOptionStaticStmt(String sql, StmtOption option) {
            super(sql, option);
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
        }

    }//MinOptionStaticStmt

    private static final class OptionQueryStaticStmt extends SingleStmtWithOption implements StaticStmt {

        private final Consumer<ResultStates> statesConsumer;

        private OptionQueryStaticStmt(String sql, Consumer<ResultStates> statesConsumer, StmtOption option) {
            super(sql, option);
            this.statesConsumer = statesConsumer;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

    }//OptionQueryStaticStmt


    protected static final class OptionStaticBatchStmt extends StmtWithOption implements StaticBatchStmt {

        private final List<String> sqlGroup;


        private OptionStaticBatchStmt(List<String> sqlGroup, StmtOption option) {
            super(option);
            this.sqlGroup = JdbdCollections.unmodifiableList(sqlGroup);
        }

        @Override
        public List<String> getSqlGroup() {
            return this.sqlGroup;
        }

    }//OptionStaticBatchStmt


    private static abstract class ParamStmtWithoutOption extends StmtWithoutOption implements ParamStmt {

        private final String sql;

        private final List<ParamValue> bindGroup;

        private ParamStmtWithoutOption(String sql, List<ParamValue> bindGroup) {
            this.sql = sql;
            this.bindGroup = JdbdCollections.unmodifiableList(bindGroup);
        }


        @Override
        public final List<ParamValue> getBindGroup() {
            return this.bindGroup;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

    }//ParamStmtWithoutOption


    private static final class MinParamStmt extends ParamStmtWithoutOption {

        private MinParamStmt(String sql, List<ParamValue> bindGroup) {
            super(sql, bindGroup);
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
        }

    }//MinParamStmt


    private static final class QueryParamStmt extends ParamStmtWithoutOption {

        private final Consumer<ResultStates> statesConsumer;

        private QueryParamStmt(String sql, List<ParamValue> bindGroup, Consumer<ResultStates> statesConsumer) {
            super(sql, bindGroup);
            this.statesConsumer = statesConsumer;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

    }//QueryParamStmt


    private static abstract class ParamStmtWithOption extends StmtWithOption implements ParamStmt {

        private final String sql;

        private final List<ParamValue> bindGroup;

        private ParamStmtWithOption(String sql, List<ParamValue> bindGroup, StmtOption option) {
            super(option);
            this.sql = sql;
            this.bindGroup = JdbdCollections.unmodifiableList(bindGroup);
        }


        @Override
        public final List<ParamValue> getBindGroup() {
            return this.bindGroup;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

    }//ParamStmtWithoutOption


    private static final class SimpleOptionParamStmt extends ParamStmtWithOption {

        private SimpleOptionParamStmt(String sql, List<ParamValue> bindGroup, StmtOption option) {
            super(sql, bindGroup, option);
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return IGNORE_RESULT_STATES;
        }

    }//SimpleOptionParamStmt


    private static final class QueryOptionParamStmt extends ParamStmtWithOption {

        private final Consumer<ResultStates> statesConsumer;

        private QueryOptionParamStmt(String sql, List<ParamValue> bindGroup, Consumer<ResultStates> statesConsumer,
                                     StmtOption option) {
            super(sql, bindGroup, option);
            this.statesConsumer = statesConsumer;
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return this.statesConsumer;
        }

    }//QueryOptionParamStmt


    private static final class MinParamBatchStmt extends StmtWithoutOption implements ParamBatchStmt {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private MinParamBatchStmt(String sql, List<List<ParamValue>> groupList) {
            this.sql = sql;
            this.groupList = JdbdCollections.unmodifiableList(groupList);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<ParamValue>> getGroupList() {
            return this.groupList;
        }


    }//MinParamBatchStmt

    private static final class OptionParamBatchStmt extends StmtWithOption implements ParamBatchStmt {

        private final String sql;

        private final List<List<ParamValue>> groupList;

        private OptionParamBatchStmt(String sql, List<List<ParamValue>> groupList, StmtOption option) {
            super(option);
            this.sql = sql;
            this.groupList = JdbdCollections.unmodifiableList(groupList);
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public List<List<ParamValue>> getGroupList() {
            return this.groupList;
        }

    }//OptionParamBatchStmt


    private static final class MinStaticMultiStmt extends StmtWithoutOption implements StaticMultiStmt {

        private final String multiStmt;

        private MinStaticMultiStmt(String multiStmt) {
            this.multiStmt = multiStmt;
        }

        @Override
        public String getMultiStmt() {
            return this.multiStmt;
        }


    }//MinStaticMultiStmt

    private static final class OptionStaticMultiStmt extends StmtWithOption implements StaticMultiStmt {

        private final String multiStmt;

        private OptionStaticMultiStmt(String multiStmt, StmtOption option) {
            super(option);
            this.multiStmt = multiStmt;
        }

        @Override
        public String getMultiStmt() {
            return this.multiStmt;
        }


    }//OptionStaticMultiStmt


    private static final class OptionMultiStmt extends StmtWithOption implements ParamMultiStmt {

        private final List<ParamStmt> stmtList;

        private OptionMultiStmt(List<ParamStmt> stmtList, StmtOption option) {
            super(option);
            this.stmtList = JdbdCollections.unmodifiableList(stmtList);
        }

        @Override
        public List<ParamStmt> getStmtList() {
            return this.stmtList;
        }

    }//OptionMultiStmt


    private static void ignoreResultStates(ResultStates states) {
        //no-op
    }

}
