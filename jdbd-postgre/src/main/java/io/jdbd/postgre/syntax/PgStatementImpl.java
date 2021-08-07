package io.jdbd.postgre.syntax;

import java.util.Collections;
import java.util.List;

final class PgStatementImpl implements PgStatement {

    static PgStatementImpl create(String sql, List<String> staticSqlList) {
        return new PgStatementImpl(sql, staticSqlList);
    }

    private final String sql;

    private final List<String> staticSqlList;

    private final int paramCount;

    private PgStatementImpl(String sql, List<String> staticSqlList) {
        this.sql = sql;
        this.staticSqlList = Collections.unmodifiableList(staticSqlList);
        this.paramCount = staticSqlList.size() - 1;
    }

    @Override
    public final List<String> getStaticSql() {
        return this.staticSqlList;
    }

    @Override
    public final int getParamCount() {
        return this.paramCount;
    }

    @Override
    public final String getSql() {
        return this.sql;
    }


}
