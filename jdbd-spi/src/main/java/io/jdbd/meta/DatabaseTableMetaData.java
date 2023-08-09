package io.jdbd.meta;


import io.jdbd.lang.Nullable;
import io.jdbd.session.Option;
import io.jdbd.session.OptionSpec;

public interface DatabaseTableMetaData extends OptionSpec {

    DatabaseSchemaMetaData schemaMeta();

    String tableName();

    /**
     * @return the comment of table
     */
    @Nullable
    String comment();


    /**
     * <p>
     * This implementation of this method must support following :
     *     <ul>
     *         <li>{@link DatabaseMetaData#TABLE_TYPE}</li>
     *     </ul>
     * </p>
     */
    @Override
    <T> T valueOf(Option<T> option);


}
