package io.jdbd.meta;

import io.jdbd.session.SessionMetaSpec;
import org.reactivestreams.Publisher;

public interface DatabaseMetaData extends SessionMetaSpec {

    Publisher<DatabaseProductMetaData> getDatabaseProduct();

    Publisher<DatabaseSchemaMetaData> getSchema();

    Publisher<DatabaseTableMetaData> getTables(DatabaseSchemaMetaData schemaMeta, String pattern);

    Publisher<TableColumnMetaData> getColumns(DatabaseTableMetaData tableMeta);

    Publisher<TableIndexMetaData> getIndexes(DatabaseTableMetaData tableMeta);


}
