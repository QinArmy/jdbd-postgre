package io.jdbd.mysql.syntax;

import io.jdbd.mysql.util.MySQLCollectionUtils;

import java.util.List;

final class DefaultMySQLStatement implements MySQLStatement {

    private final String sql;

    private final List<String> staticSqlList;

    private final boolean loadInfile;

    DefaultMySQLStatement(String sql, List<String> staticSqlList, boolean loadInfile) {
        this.sql = sql;
        this.staticSqlList = MySQLCollectionUtils.unmodifiableList(staticSqlList);
        this.loadInfile = loadInfile;
    }

    @Override
    public List<String> getStaticSql() {
        return this.staticSqlList;
    }

    @Override
    public int getParamCount() {
        return this.staticSqlList.size() - 1;
    }

    @Override
    public String getSql() {
        return this.sql;
    }

    @Override
    public boolean isLoadInfile() {
        return this.loadInfile;
    }
}
