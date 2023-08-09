package io.jdbd.meta;


import io.jdbd.lang.Nullable;
import io.jdbd.session.OptionSpec;

public interface TableColumnMetaData extends OptionSpec {

    DatabaseTableMetaData tableMeta();

    String columnName();

    DataType dataType();

    long precision();

    int scale();

    boolean isNullable();

    /**
     * @return 'null' representing default is null
     */
    @Nullable
    String defaultValue();

    @Nullable
    String comment();


}
