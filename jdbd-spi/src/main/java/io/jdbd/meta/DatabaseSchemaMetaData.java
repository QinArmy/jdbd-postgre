package io.jdbd.meta;


import io.jdbd.lang.Nullable;
import io.jdbd.session.OptionSpec;

public interface DatabaseSchemaMetaData extends OptionSpec {

    /**
     * @return the {@link DatabaseMetaData} that create this instance .
     */
    DatabaseMetaData databaseMeta();

    @Nullable
    String catalog();

    @Nullable
    String schema();


}
