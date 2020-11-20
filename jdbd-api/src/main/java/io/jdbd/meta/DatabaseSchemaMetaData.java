package io.jdbd.meta;

import io.jdbd.lang.Nullable;

public final class DatabaseSchemaMetaData {

    private final String catalog;

    private final String schema;

    public DatabaseSchemaMetaData(@Nullable String catalog, String schema) {
        this.catalog = catalog;
        this.schema = schema;
    }

    @Nullable
    public String getCatalog() {
        return this.catalog;
    }

    public String getSchema() {
        return this.schema;
    }
}
