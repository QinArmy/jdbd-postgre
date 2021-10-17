package io.jdbd.postgre.config;

import io.jdbd.vendor.conf.AbstractHostInfo;
import io.jdbd.vendor.conf.ImmutableMapProperties;
import io.jdbd.vendor.conf.JdbcUrlParser;
import io.jdbd.vendor.conf.Properties;
import org.qinarmy.env.convert.ConverterManager;
import org.qinarmy.env.convert.ImmutableConverterManager;

import java.util.Map;

public class PgHost extends AbstractHostInfo {

    public static PgHost create(JdbcUrlParser parser, int index) {
        return new PgHost(parser, index);
    }


    public static final int DEFAULT_PORT = 5432;


    private PgHost(JdbcUrlParser parser, int index) {
        super(parser, index);
    }

    @Override
    protected final PgKey getUserKey() {
        return PgKey.user;
    }

    @Override
    protected final PgKey getPasswordKey() {
        return PgKey.password;
    }

    @Override
    protected final PgKey getHostKey() {
        return PgKey.PGHOST;
    }

    @Override
    protected final PgKey getPortKey() {
        return PgKey.PGPORT;
    }

    @Override
    protected final PgKey getDbNameKey() {
        return PgKey.PGDBNAME;
    }

    @Override
    protected final int getDefaultPort() {
        return DEFAULT_PORT;
    }

    public String getNonNullDbName() {
        String dbName = this.dbName;
        return dbName == null ? "" : dbName;
    }


    @Override
    protected Properties createProperties(Map<String, String> map) {
        ConverterManager converterManager = ImmutableConverterManager.create(Converters::registerConverter);
        return ImmutableMapProperties.getInstance(map, converterManager);
    }


}
