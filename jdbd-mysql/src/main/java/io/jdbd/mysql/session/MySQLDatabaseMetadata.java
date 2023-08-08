package io.jdbd.mysql.session;

import io.jdbd.meta.*;
import io.jdbd.mysql.protocol.MySQLProtocol;
import org.reactivestreams.Publisher;

final class MySQLDatabaseMetadata extends MySQLSessionMetaSpec implements DatabaseMetaData {

    static MySQLDatabaseMetadata create(MySQLProtocol protocol) {
        return new MySQLDatabaseMetadata(protocol);
    }

    private MySQLDatabaseMetadata(MySQLProtocol protocol) {
        super(protocol);
    }


    @Override
    public Publisher<DatabaseProductMetaData> getDatabaseProduct() {
        return null;
    }

    @Override
    public Publisher<DatabaseSchemaMetaData> getSchema() {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> getTables(DatabaseSchemaMetaData schemaMeta, String pattern) {
        return null;
    }

    @Override
    public Publisher<TableColumnMetaData> getColumns(DatabaseTableMetaData tableMeta) {
        return null;
    }

    @Override
    public Publisher<TableIndexMetaData> getIndexes(DatabaseTableMetaData tableMeta) {
        return null;
    }
}
