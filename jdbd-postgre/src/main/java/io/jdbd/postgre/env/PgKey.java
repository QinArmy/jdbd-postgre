package io.jdbd.postgre.env;

import io.jdbd.lang.Nullable;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.vendor.env.Key;
import io.jdbd.vendor.env.Redefine;
import io.jdbd.vendor.task.SslMode;

import java.nio.file.Path;
import java.util.List;


/**
 * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">properties</a>
 */
public final class PgKey<T> extends Key<T> {

    public static final PgKey<String> k = new PgKey<>("", String.class, null);


    public static final PgKey<String> OPTIONS = new PgKey<>("options", String.class, null);

    // ssl group

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sslfactory(Class .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    @Deprecated
    sslfactoryarg(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    @Redefine
    sslmode(SslMode .class, "DISABLED"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sslcert(Path .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sslkey(Path .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sslrootcert(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sslhostnameverifier(Class .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sslpasswordcallback(Class .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sslpassword(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    //
    protocolVersion(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    @Deprecated
    loggerLevel(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    @Deprecated
    loggerFile(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    allowEncodingChanges(Boolean .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    logUnclosedConnections(Boolean .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    autosave(Enums.AutoSave .class, "never"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    cleanupSavepoints(Boolean .class, "false"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    binaryTransfer(Boolean .class, "true"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    binaryTransferEnable(String .class),

    binaryTransferDisable(String .class),

    databaseMetadataCacheFields(Integer .class, "65536"),

    databaseMetadataCacheFieldsMiB(Integer .class, "5"),

    prepareThreshold(Integer .class),

    preparedStatementCacheQueries(Integer .class),

    preparedStatementCacheSizeMiB(Integer .class),
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

    trustStorePassword(String .class),

    fallbackToSystemKeyStore(Boolean .class, "false"),

    factoryWorkerCount(Integer .class, "100"),

    hexEscapes(Boolean .class, "false"),

    lc_monetary(String .class);


    private static final List<PgKey<?>> KEY_LIST = createKeyList();

    /**
     * <p>
     * private constructor.
     * </p>
     */
    private PgKey(String name, Class<T> valueClass, @Nullable T defaultValue) {
        super(name, valueClass, defaultValue);
    }


    public static List<PgKey<?>> keyList() {
        return KEY_LIST;
    }


    @SuppressWarnings("unchecked")
    private static List<PgKey<?>> createKeyList() {
        final List<PgKey<?>> keyList = PgCollections.arrayList();
        try {
            addAllKey(PgKey.class, keyList::add);

            return PgCollections.asUnmodifiableList(keyList);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }


}
