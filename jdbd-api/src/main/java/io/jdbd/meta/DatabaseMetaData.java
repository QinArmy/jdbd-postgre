package io.jdbd.meta;

import io.jdbd.DatabaseProductMetaData;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface DatabaseMetaData {

    Mono<DatabaseProductMetaData> getDatabaseProduct();

    Mono<DatabaseSchemaMetaData> getSchema();

    Flux<DatabaseTableMetaData> getTables(DatabaseSchemaMetaData schemaMeta,String pattern);

    Flux<TableColumnMetaData> getColumns(DatabaseTableMetaData tableMeta);

    Flux<TableIndexMetaData> getIndexes(DatabaseTableMetaData tableMeta);

    boolean supportSavePoints();

}
