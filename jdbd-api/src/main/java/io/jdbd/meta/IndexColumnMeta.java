package io.jdbd.meta;

public final class IndexColumnMeta {

    private final String columnName;

    private final Sorting sorting;

    public IndexColumnMeta(String columnName, Sorting sorting) {
        this.columnName = columnName;
        this.sorting = sorting;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public Sorting getSorting() {
        return this.sorting;
    }
}
