package io.jdbd.meta;

import io.jdbd.lang.Nullable;

public final class DatabaseTableMetaData {

    private final DatabaseSchemaMetaData schemaMeta;

    private final String tableName;

    private final String comment;

    public DatabaseTableMetaData(DatabaseSchemaMetaData schemaMeta, String tableName, @Nullable String comment) {
        this.schemaMeta = schemaMeta;
        this.tableName = tableName;
        this.comment = comment;
    }

    public DatabaseSchemaMetaData getSchemaMeta() {
        return this.schemaMeta;
    }

    public String getTableName() {
        return this.tableName;
    }

    @Nullable
    public String getComment() {
        return this.comment;
    }
}
