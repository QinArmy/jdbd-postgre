package io.jdbd.postgre.env;

import io.jdbd.Driver;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.env.SimpleEnvironment;

import java.util.Map;


final class PostgreHost implements PgHost {


    /**
     * @param propMap a modified map, because will remove some key.
     */
    static PgHost create(final Map<String, Object> propMap) {
        final PostgreHost host;
        host = new PostgreHost(propMap);
        final Environment env = host.env;
        // check all key
        for (PgKey<?> key : PgKey.keyList()) {
            env.get(key);
        }
        return host;
    }

    private final String host;

    private final int port;

    private final String dbName;

    private final String user;

    private final String password;

    private final Environment env;


    private PostgreHost(final Map<String, Object> propMap) {

        this.host = (String) propMap.remove(HOST);
        this.port = (Integer) propMap.remove(PORT);
        this.dbName = (String) propMap.remove(DB_NAME);

        this.user = (String) propMap.remove(Driver.USER);
        this.password = (String) propMap.remove(Driver.PASSWORD);

        if (this.host == null || this.user == null) {
            // no bug, never here
            throw new IllegalStateException("propMap error");
        }

        this.env = SimpleEnvironment.from(propMap);
    }

    @Override
    public String host() {
        return this.host;
    }

    @Override
    public int port() {
        return this.port;
    }

    @Override
    public String user() {
        return this.user;
    }

    @Override
    public String password() {
        return this.password;
    }

    @Override
    public String dbName() {
        return this.dbName;
    }

    @Override
    public Environment properties() {
        return this.env;
    }


    @Override
    public String toString() {
        return PgStrings.builder()
                .append(getClass().getName())
                .append("[ host : ")
                .append(this.host)
                .append(" , port : ")
                .append(this.port)
                .append(" , database")
                .append(this.dbName)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
