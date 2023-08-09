package io.jdbd.postgre.session;

import io.jdbd.DriverVersion;
import io.jdbd.meta.*;
import io.jdbd.postgre.PgDriver;
import io.jdbd.postgre.protocol.client.PgProtocol;
import io.jdbd.session.Option;
import org.reactivestreams.Publisher;

import java.util.Map;

/**
 * <p>
 * This class is a implementation of {@link DatabaseMetaData} with postgre client protocol.
 * </p>
 *
 * @since 1.0
 */
final class PgDatabaseMetaData extends PgDatabaseMetaSpec implements DatabaseMetaData {

    static PgDatabaseMetaData create(PgProtocol protocol) {
        return new PgDatabaseMetaData(protocol);
    }

    private PgDatabaseMetaData(PgProtocol protocol) {
        super(protocol);
    }

    @Override
    public String productFamily() {
        return PgDriver.POSTGRE_SQL;
    }

    @Override
    public String productName() {
        return PgDriver.POSTGRE_SQL;
    }

    @Override
    public DriverVersion driverVersion() {
        return PgDriver.getInstance().version();
    }

    @Override
    public Publisher<DatabaseSchemaMetaData> currentSchema() {
        return null;
    }

    @Override
    public Publisher<DatabaseSchemaMetaData> schemas() {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tableOfCurrentSchema() {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tableOfSchema(DatabaseSchemaMetaData schemaMeta) {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tableOfSchema(DatabaseSchemaMetaData schemaMeta, Map<Option<?>, ?> optionPairMap) {
        return null;
    }

    @Override
    public Publisher<TableColumnMetaData> columnOfTable(DatabaseTableMetaData tableMeta, Map<Option<?>, ?> optionPairMap) {
        return null;
    }

    @Override
    public Publisher<TableIndexMetaData> indexOfTable(DatabaseTableMetaData tableMeta, Map<Option<?>, ?> optionPairMap) {
        return null;
    }

    @Override
    public Publisher<DatabaseTableMetaData> tables(Map<Option<?>, ?> optionPairMap) {
        return null;
    }

    @Override
    public Publisher<TableColumnMetaData> columns(Map<Option<?>, ?> optionPairMap) {
        return null;
    }

    @Override
    public Publisher<TableIndexMetaData> indexes(Map<Option<?>, ?> optionPairMap) {
        return null;
    }

    @Override
    public <R> Publisher<R> queryOption(Option<R> option) {
        return null;
    }

    @Override
    public Publisher<String> sqlKeyWords() {
        return null;
    }


}
