package io.jdbd.postgre.stmt;

import io.jdbd.postgre.PgType;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.postgre.util.PgFunctions;
import io.jdbd.result.ResultState;
import io.jdbd.vendor.stmt.JdbdStmts;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class PgStmts extends JdbdStmts {


    public static BindableStmt single(String sql, PgType type, @Nullable Object value) {
        return new BindableStmtForSimple(sql, Collections.singletonList(BindValue.create(0, type, value)));
    }

    public static BindableStmt bindable(String sql, List<BindValue> paramGroup) {
        return new BindableStmtForSimple(sql, paramGroup);
    }


    private static final class BindableStmtForSimple implements BindableStmt {

        private final String sql;

        private final List<BindValue> paramGroup;

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


}
