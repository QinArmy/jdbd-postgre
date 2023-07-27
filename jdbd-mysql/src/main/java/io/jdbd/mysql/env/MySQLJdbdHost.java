package io.jdbd.mysql.env;

import io.jdbd.Driver;

import java.util.Map;

final class MySQLJdbdHost implements MySQLHost {


    static MySQLJdbdHost create(Protocol protocol, Map<String, Object> properties) {
        return new MySQLJdbdHost(protocol, properties);
    }


    private final Protocol protocol;

    private final String host;

    private final int port;

    private final String user;

    private final String password;

    private final String dbName;

    private final Environment env;

    private MySQLJdbdHost(final Protocol protocol, final Map<String, Object> properties) {
        this.protocol = protocol;
        this.host = (String) properties.remove(MySQLKey.HOST.name);
        this.port = (Integer) properties.remove(MySQLKey.PORT.name);
        this.user = (String) properties.remove(Driver.USER);

        this.password = (String) properties.remove(Driver.PASSWORD);
        this.dbName = (String) properties.remove(MySQLKey.DB_NAME.name);

        this.env = MySQLEnvironment.from(properties);
    }


    @Override
    public Protocol protocol() {
        return this.protocol;
    }


    @Override
    public String getHost() {
        return this.host;
    }

    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public String getUser() {
        return this.user;
    }


    @Override
    public String getPassword() {
        return this.password;
    }

    @Override
    public String getDbName() {
        return this.dbName;
    }


    @Override
    public Environment properties() {
        return this.env;
    }


}
