package io.jdbd.postgre.stmt;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.stmt.JdbdStmts;
import org.reactivestreams.Publisher;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class PgStmts extends JdbdStmts {


    public static BindableStmt single(String sql, PgType type, @Nullable Object value) {
        return new BindableStmtForSimple(sql, Collections.singletonList(BindValue.create(0, type, value)));
    }

    public static BindableStmt bindable(String sql, List<BindValue> paramGroup) {
        return new BindableStmtForSimple(sql, paramGroup);
    }

    public static BindableStmt bindable(String sql, BindValue param) {
        return new BindableStmtForSimple(sql, param);
    }

    public static BindableStmt bindableWithImport(String sql, BindValue param
            , Function<String, Publisher<byte[]>> function) {
        return new BindableStmtWithImport(sql, param, function);
    }

    public static BatchBindStmt bindableBatch(String sql, List<List<BindValue>> groupList) {
        return new BatchBindStmtImpl(sql, groupList, 0);
    }

    public static BatchBindStmt bindableBatch(String sql, List<List<BindValue>> groupList, int timeout) {
        return new BatchBindStmtImpl(sql, groupList, timeout);
    }

    public static MultiBindStmt multi(List<BindableStmt> stmtGroup) {
        return new MultiBindStmtImpl(stmtGroup, 0);
    }

    private static final class BindableStmtForSimple implements BindableStmt {

        private final String sql;

        private final List<BindValue> paramGroup;

        private BindableStmtForSimple(String sql, BindValue param) {
            this.sql = sql;
            this.paramGroup = Collections.singletonList(param);
        }

        private BindableStmtForSimple(String sql, List<BindValue> paramGroup) {
            this.sql = sql;
            this.paramGroup = PgCollections.unmodifiableList(paramGroup);
        }

        @Override
        public final List<BindValue> getParamGroup() {
            return this.paramGroup;
        }

        @Override
        public final int getFetchSize() {
            return 0;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return PgFunctions.noActionConsumer();
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

    }

    private static final class BindableStmtWithImport implements BindableStmt {

        private final String sql;

        private final List<BindValue> paramGroup;

        private final Function<String, Publisher<byte[]>> importFunction;

        private BindableStmtWithImport(String sql, BindValue param
                , Function<String, Publisher<byte[]>> importFunction) {
            this.sql = sql;
            this.paramGroup = Collections.singletonList(param);
            this.importFunction = Objects.requireNonNull(importFunction);
        }

        private BindableStmtWithImport(String sql, List<BindValue> paramGroup
                , Function<String, Publisher<byte[]>> importFunction) {
            this.sql = sql;
            this.paramGroup = Collections.unmodifiableList(paramGroup);
            this.importFunction = Objects.requireNonNull(importFunction);
        }

        @Override
        public final List<BindValue> getParamGroup() {
            return this.paramGroup;
        }

        @Override
        public final int getFetchSize() {
            return 0;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final Consumer<ResultState> getStatusConsumer() {
            return PgFunctions.noActionConsumer();
        }

        @Override
        public final int getTimeout() {
            return 0;
        }

    }

    private static final class BatchBindStmtImpl implements BatchBindStmt {

        private final String sql;

        private final List<List<BindValue>> groupList;

        private final int timeout;


        private BatchBindStmtImpl(String sql, List<List<BindValue>> groupList, int timeout) {
            this.sql = sql;
            this.groupList = Collections.unmodifiableList(groupList);
            this.timeout = timeout;
        }

        @Override
        public final List<List<BindValue>> getGroupList() {
            return this.groupList;
        }

        @Override
        public final String getSql() {
            return this.sql;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }


    }

    private static final class MultiBindStmtImpl implements MultiBindStmt {

        private final List<BindableStmt> stmtGroup;

        private final int timeout;

        private MultiBindStmtImpl(List<BindableStmt> stmtGroup, int timeout) {
            this.stmtGroup = Collections.unmodifiableList(stmtGroup);
            this.timeout = timeout;
        }

        @Override
        public final List<BindableStmt> getStmtGroup() {
            return this.stmtGroup;
        }

        @Override
        public final int getTimeout() {
            return this.timeout;
        }

    }


}
