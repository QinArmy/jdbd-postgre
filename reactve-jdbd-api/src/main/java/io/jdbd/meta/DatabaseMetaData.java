package io.jdbd.meta;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface DatabaseMetaData {

    Mono<DatabaseSchemaMeta> getSchema();

   Flux<DatabaseTableMeta> getTables();

   Flux<TableColumnMeta> getColumns(DatabaseTableMeta tableMeta);

   Flux<TableIndexMeta> getIndexes(DatabaseTableMeta tableMeta);


   boolean supportSavePoints();

}
