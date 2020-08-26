package io.jdbd.meta;

import io.jdbd.lang.Nullable;

public final class TableColumnMetaData {

    private final DatabaseTableMetaData tableMeta;

    private final String columnName;

    private final  String dataTypeName;

    private final long precision;

    private final int scale;

    private final String defaultValue;

    private final String comment;

    public TableColumnMetaData(DatabaseTableMetaData tableMeta, String columnName, String dataTypeName, long precision
            , int scale, @Nullable  String defaultValue, @Nullable String comment) {
        this.tableMeta = tableMeta;
        this.columnName = columnName;
        this.dataTypeName = dataTypeName;
        this.precision = precision;
        this.scale = scale;
        this.defaultValue = defaultValue;
        this.comment = comment;
    }


    public DatabaseTableMetaData getTableMeta() {
        return this.tableMeta;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public String getDataTypeName() {
        return this.dataTypeName;
    }

    public long getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }

    @Nullable
    public String getDefaultValue() {
        return this.defaultValue;
    }

    @Nullable
    public String getComment() {
        return this.comment;
    }
}
