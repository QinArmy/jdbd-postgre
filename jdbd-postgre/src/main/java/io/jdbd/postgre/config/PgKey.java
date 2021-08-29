package io.jdbd.postgre.config;

import io.jdbd.vendor.conf.IPropertyKey;
import io.jdbd.vendor.conf.Redefine;
import io.jdbd.vendor.task.SslMode;
import reactor.util.annotation.Nullable;

import java.nio.file.Path;

/**
 * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">properties</a>
 */
public enum PgKey implements IPropertyKey {

    user(String.class),
    password(String.class),
    options(String.class),

    // ssl group
    ssl(Boolean.class),
    sslfactory(Class.class),
    @Deprecated
    sslfactoryarg(String.class),
    @Redefine
    sslmode(SslMode.class, "DISABLED"),

    sslcert(Path.class),
    sslkey(Path.class),
    sslrootcert(String.class),
    sslhostnameverifier(Class.class),

    sslpasswordcallback(Class.class),
    sslpassword(String.class),

    //
    protocolVersion(Integer.class),
    @Deprecated
    loggerLevel(String.class),
    @Deprecated
    loggerFile(String.class),
    allowEncodingChanges(Boolean.class),

    logUnclosedConnections(Boolean.class),
    autosave(Enums.AutoSave.class, "never"),
    cleanupSavepoints(Boolean.class, "false"),
    binaryTransfer(Boolean.class, "true"),

    binaryTransferEnable(String.class),
    binaryTransferDisable(String.class),
    databaseMetadataCacheFields(Integer.class, "65536"),
    databaseMetadataCacheFieldsMiB(Integer.class, "5"),

    prepareThreshold(Integer.class),

    preparedStatementCacheQueries(Integer.class),
    preparedStatementCacheSizeMiB(Integer.class),
    preferQueryMode(String.class),

    defaultRowFetchSize(Integer.class),
    loginTimeout(Integer.class),
    connectTimeout(Integer.class),
    socketTimeout(Integer.class),

    cancelSignalTimeout(Integer.class),
    tcpKeepAlive(Integer.class),
    unknownLength(Integer.class),
    stringtype(Integer.class),

    ApplicationName(String.class),
    kerberosServerName(Integer.class),
    jaasApplicationName(Integer.class),
    jaasLogin(Integer.class, "true"),

    gssEncMode(Enums.GSSEncMode.class, "DISABLE"),
    gsslib(Integer.class),
    // Since: 9.4
    sspiServiceClass(Integer.class),
    useSpnego(Boolean.class, "false"),

    sendBufferSize(Integer.class),
    receiveBufferSize(Integer.class),
    readOnly(Integer.class),
    readOnlyMode(Integer.class),

    disableColumnSanitiser(Integer.class),
    assumeMinServerVersion(String.class),
    currentSchema(Integer.class),
    targetServerType(Integer.class),

    hostRecheckSeconds(Integer.class),
    loadBalanceHosts(Integer.class),
    socketFactory(Integer.class),
    socketFactoryArg(Integer.class),

    reWriteBatchedInserts(Integer.class),
    replication(Integer.class),
    escapeSyntaxCallMode(Integer.class),
    maxResultBuffer(Integer.class),
    hideUnprivilegedObjects(String.class),
    logServerErrorDetail(String.class),

    PGDBNAME("PGDBNAME", String.class, null),
    PGHOST("PGHOST", String.class, null),
    PGPORT("PGPORT", String.class, null),
    xmlFactoryFactory(String.class),

    keyStoreType(String.class),
    keyStoreUrl(String.class),
    keyStorePassword(String.class),

    trustStoreType(String.class),
    trustStoreUrl(String.class),
    trustStorePassword(String.class),

    fallbackToSystemKeyStore(Boolean.class, "false");

    private final String key;

    private final String defaultValue;

    private final Class<?> javaType;


    PgKey(Class<?> javaType) {
        this(null, javaType, null);
    }


    PgKey(Class<?> javaType, @Nullable String defaultValue) {
        this(null, javaType, defaultValue);
    }

    PgKey(@Nullable String key, Class<?> javaType, @Nullable String defaultValue) {
        this.key = key == null ? this.name() : key;
        this.javaType = javaType;
        this.defaultValue = defaultValue;
    }


    @Override
    public final String getKey() {
        return this.key;
    }

    @Nullable
    @Override
    public final String getAlias() {
        return null;
    }

    @Nullable
    @Override
    public final String getDefault() {
        return this.defaultValue;
    }

    @Override
    public final Class<?> getJavaType() {
        return this.javaType;
    }

    @Override
    public final boolean isCaseSensitive() {
        return true;
    }

}
