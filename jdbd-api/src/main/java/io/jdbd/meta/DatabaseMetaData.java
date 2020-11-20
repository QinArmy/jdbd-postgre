package io.jdbd.meta;

import io.jdbd.DatabaseProductMetaData;
import org.reactivestreams.Publisher;

public interface DatabaseMetaData {

    Publisher<DatabaseProductMetaData> getDatabaseProduct();

    Publisher<DatabaseSchemaMetaData> getSchema();

    Publisher<DatabaseTableMetaData> getTables(DatabaseSchemaMetaData schemaMeta, String pattern);

    Publisher<TableColumnMetaData> getColumns(DatabaseTableMetaData tableMeta);

    Publisher<TableIndexMetaData> getIndexes(DatabaseTableMetaData tableMeta);

    boolean supportSavePoints();

}
