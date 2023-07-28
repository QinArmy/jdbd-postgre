package io.jdbd.postgre.stmt;

import io.jdbd.postgre.PgType;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.JdbdStmts;
import io.jdbd.vendor.stmt.StaticBatchStmt;
import io.jdbd.vendor.stmt.StaticStmt;
import io.jdbd.vendor.stmt.StmtOption;
import io.jdbd.vendor.util.JdbdFunctions;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class PgStmts extends JdbdStmts {

    public static StaticStmt stmt(String sql, StmtOption option) {
        Objects.requireNonNull(sql, "sql");
        return new PgOptionStaticStmt(sql, option);
    }

    public static StaticStmt stmt(String sql, Consumer<ResultStates> statesConsumer, StmtOption option) {
        Objects.requireNonNull(sql, "sql");
        return new PgOptionQueryStaticStmt(sql, statesConsumer, option);
    }

    public static StaticBatchStmt batch(final List<String> sqlGroup, final StmtOption option) {
        return new PgOptionStaticBatchStmt(sqlGroup, option);
    }

    public static BindStmt single(String sql, PgType type, @Nullable final Object value) {
        return new PgSimpleBindStmt(sql, Collections.singletonList(BindValue.wrap(0, type, value)));
    }

    @Deprecated
    public static BindStmt single(String sql, BindValue bindValue) {
        return new PgSimpleBindStmt(sql, Collections.singletonList(bindValue));
    }

    /**
     * This class used by {@link io.jdbd.statement.MultiStatement}, so if paramGroup is empty
     * ,just do {@link Collections#unmodifiableList(List)}.
     */
    public static BindStmt bind(String sql, List<BindValue> bindGroup) {
        return new PgSimpleBindStmt(sql, bindGroup);
    }

    public static BindStmt bind(String sql, List<BindValue> paramGroup, Consumer<ResultStates> statesConsumer) {
        return new PgSimpleQueryBindStmt(sql, paramGroup, statesConsumer);
    }

    public static BindBatchStmt bindBatch(String sql, List<List<BindValue>> groupList) {
        return new PgSimpleBindBatchStmt(sql, groupList);
    }

    public static BindStmt bind(String sql, List<BindValue> paramGroup, StmtOption option) {
        Objects.requireNonNull(sql, "sql");
        return new PgOptionBindStmt(sql, paramGroup, option);
    }

    public static BindStmt bind(String sql, List<BindValue> paramGroup, Consumer<ResultStates> statesConsumer
            , StmtOption option) {
        Objects.requireNonNull(sql, "sql");
        return new PgOptionQueryBindStmt(sql, paramGroup, statesConsumer, option);
    }


    public static BindBatchStmt bindBatch(String sql, List<List<BindValue>> groupList, StmtOption option) {
        Objects.requireNonNull(sql, "sql");
        return new PgOptionBindBatchStmt(sql, groupList, option);
    }

    public static BindMultiStmt multi(List<BindStmt> stmtGroup) {
        return new PgSimpleBindMultiStmt(stmtGroup);
    }

    public static BindMultiStmt multi(List<BindStmt> stmtGroup, StmtOption option) {
        return new PgOptionBindMultiStmt(stmtGroup, option);
    }


    private static final class PgOptionStaticStmt extends OptionStaticStmt {

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private PgOptionStaticStmt(String sql, StmtOption option) {
            super(sql, option);
            this.importPublisher = option.getImportFunction();
            this.exportSubscriber = option.getExportFunction();
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return this.importPublisher;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return this.exportSubscriber;
        }

    }

    private static final class PgOptionQueryStaticStmt extends OptionQueryStaticStmt {

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private PgOptionQueryStaticStmt(String sql, Consumer<ResultStates> statesConsumer, StmtOption option) {
            super(sql, statesConsumer, option);
            this.importPublisher = option.getImportFunction();
            this.exportSubscriber = option.getExportFunction();
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return this.importPublisher;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return this.exportSubscriber;
        }

    }


    private static final class PgOptionStaticBatchStmt extends OptionStaticBatchStmt {

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private PgOptionStaticBatchStmt(List<String> sqlGroup, StmtOption option) {
            super(sqlGroup, option);
            if (this.sqlGroup.size() == 1) {
                this.importPublisher = option.getImportFunction();
                this.exportSubscriber = option.getExportFunction();
            } else {
                this.importPublisher = null;
                this.exportSubscriber = null;
            }
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return this.importPublisher;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return this.exportSubscriber;
        }

    }

    private static final class PgSimpleBindStmt extends SimpleParamStmt<BindValue> implements BindStmt {

        private PgSimpleBindStmt(String sql, List<BindValue> bindGroup) {
            super(sql, bindGroup);
        }
    }

    private static final class PgSimpleQueryBindStmt extends SimpleQueryParamStmt<BindValue> implements BindStmt {

        private PgSimpleQueryBindStmt(String sql, List<BindValue> bindGroup, Consumer<ResultStates> statesConsumer) {
            super(sql, bindGroup, statesConsumer);
        }

    }

    private static final class PgSimpleBindBatchStmt extends SimpleParamBatchStmt<BindValue> implements BindBatchStmt {

        private PgSimpleBindBatchStmt(String sql, List<List<BindValue>> groupList) {
            super(sql, groupList);
        }

    }

    private static final class PgOptionBindStmt extends OptionParamStmt<BindValue> implements BindStmt {

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private PgOptionBindStmt(String sql, List<BindValue> bindGroup, StmtOption option) {
            super(sql, bindGroup, option);
            this.importPublisher = option.getImportFunction();
            this.exportSubscriber = option.getExportFunction();
        }

        @Override
        public Consumer<ResultStates> getStatusConsumer() {
            return JdbdFunctions.noActionConsumer();
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return this.importPublisher;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return this.exportSubscriber;
        }

    }

    private static final class PgOptionQueryBindStmt extends OptionQueryParamStmt<BindValue> implements BindStmt {

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private PgOptionQueryBindStmt(String sql, List<BindValue> bindGroup
                , Consumer<ResultStates> statesConsumer, StmtOption option) {
            super(sql, bindGroup, statesConsumer, option);
            this.importPublisher = option.getImportFunction();
            this.exportSubscriber = option.getExportFunction();
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return this.importPublisher;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return this.exportSubscriber;
        }

    }

    private static final class PgOptionBindBatchStmt extends OptionParamBatchStmt<BindValue> implements BindBatchStmt {

        private final Function<Object, Publisher<byte[]>> importPublisher;

        private final Function<Object, Subscriber<byte[]>> exportSubscriber;

        private PgOptionBindBatchStmt(String sql, List<List<BindValue>> groupList, StmtOption option) {
            super(sql, groupList, option);
            if (this.groupList.size() == 1) {
                this.importPublisher = option.getImportFunction();
                this.exportSubscriber = option.getExportFunction();
            } else {
                this.importPublisher = null;
                this.exportSubscriber = null;
            }
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return this.importPublisher;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return this.exportSubscriber;
        }

    }


    private static final class PgSimpleBindMultiStmt implements BindMultiStmt {

        private final List<BindStmt> stmtGroup;

        private PgSimpleBindMultiStmt(List<BindStmt> stmtGroup) {
            this.stmtGroup = wrapGroup(stmtGroup);
        }

        @Override
        public List<BindStmt> getStmtList() {
            return this.stmtGroup;
        }

        @Override
        public int getTimeout() {
            return 0;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return null;
        }

    }

    private static final class PgOptionBindMultiStmt implements BindMultiStmt {

        private final List<BindStmt> stmtList;

        private final int timeout;

        public PgOptionBindMultiStmt(List<BindStmt> stmtList, StmtOption option) {
            this.stmtList = wrapGroup(stmtList);
            this.timeout = option.getTimeout();
        }

        @Override
        public List<BindStmt> getStmtList() {
            return this.stmtList;
        }

        @Override
        public int getTimeout() {
            return this.timeout;
        }

        @Override
        public int getFetchSize() {
            return 0;
        }

        @Override
        public Function<Object, Publisher<byte[]>> getImportFunction() {
            return null;
        }

        @Override
        public Function<Object, Subscriber<byte[]>> getExportFunction() {
            return null;
        }

    }


}
