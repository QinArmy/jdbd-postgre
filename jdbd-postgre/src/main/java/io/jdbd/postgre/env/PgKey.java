package io.jdbd.postgre.env;

import io.jdbd.lang.Nullable;
import io.jdbd.postgre.util.PgCollections;
import io.jdbd.vendor.env.Key;
import io.jdbd.vendor.task.SslMode;

import java.util.List;


/**
 * @see <a href="https://jdbc.postgresql.org/documentation/head/connect.html">properties</a>
 */
public final class PgKey<T> extends Key<T> {

    public static final PgKey<String> k = new PgKey<>("", String.class, null);


    public static final PgKey<String> OPTIONS = new PgKey<>("options", String.class, null);

    // ssl group

    public static final PgKey<String> SSL_FACTORY = new PgKey<>("sslFactory", String.class, null);


    public static final PgKey<SslMode> SSL_MODE = new PgKey<>("sslMode", SslMode.class, SslMode.DISABLED);


    public static final PgKey<String> SSL_CERT = new PgKey<>("sslCert", String.class, null);

    public static final PgKey<String> SSL_KEY = new PgKey<>("ssl_key", String.class, null);


    public static final PgKey<String> SSL_ROOT_CERT = new PgKey<>("sslRootCert", String.class, null);


    public static final PgKey<String> SSL_HOST_NAME_VERIFIER = new PgKey<>("sslHostNameVerifier", String.class, null);

    public static final PgKey<String> sslPasswordCallback = new PgKey<>("sslPasswordCallback", String.class, null);

    public static final PgKey<String> SSL_PASSWORD = new PgKey<>("sslPassword", String.class, null);

    public static final PgKey<String> PROTOCOL_VERSION = new PgKey<>("protocolVersion", String.class, null);

    public static final PgKey<Boolean> ALLOW_ENCODING_CHANGES = new PgKey<>("allowEncodingChanges", Boolean.class, Boolean.FALSE);

    public static final PgKey<Boolean> LOG_UNCLOSED_CONNECTIONS = new PgKey<>("logUnclosedConnections", Boolean.class, Boolean.FALSE);

    public static final PgKey<Enums.AutoSave> AUTO_SAVE = new PgKey<>("autoSave", Enums.AutoSave.class, Enums.AutoSave.never);

    public static final PgKey<Boolean> cleanupSavePoints = new PgKey<>("cleanupSavePoints", Boolean.class, Boolean.FALSE);


    public static final PgKey<Boolean> BINARY_TRANSFER = new PgKey<>("binaryTransfer", Boolean.class, Boolean.TRUE);


    binaryTransferEnable(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    binaryTransferDisable(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    databaseMetadataCacheFields(Integer .class, "65536"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    databaseMetadataCacheFieldsMiB(Integer .class, "5"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    prepareThreshold(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    preparedStatementCacheQueries(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    preparedStatementCacheSizeMiB(Integer .class),


    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    preferQueryMode(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    defaultRowFetchSize(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    loginTimeout(Integer .class),


    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    connectTimeout(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    socketTimeout(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    cancelSignalTimeout(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    tcpKeepAlive(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    unknownLength(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    stringtype(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    ApplicationName(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    kerberosServerName(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    jaasApplicationName(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    jaasLogin(Integer .class, "true"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    gssEncMode(Enums.GSSEncMode .class, "DISABLE"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    gsslib(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    // Since: 9.4
    sspiServiceClass(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    useSpnego(Boolean .class, "false"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    sendBufferSize(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    receiveBufferSize(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    readOnly(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    readOnlyMode(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    disableColumnSanitiser(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    assumeMinServerVersion(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    currentSchema(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    targetServerType(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    hostRecheckSeconds(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    loadBalanceHosts(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    socketFactory(Integer .class),


    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    socketFactoryArg(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    reWriteBatchedInserts(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    replication(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    escapeSyntaxCallMode(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    maxResultBuffer(Integer .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    hideUnprivilegedObjects(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    logServerErrorDetail(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);


    xmlFactoryFactory(String .class),


    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    keyStoreType(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    keyStoreUrl(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    keyStorePassword(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    trustStoreType(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    trustStoreUrl(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    trustStorePassword(String .class),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    fallbackToSystemKeyStore(Boolean .class, "false"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    factoryWorkerCount(Integer .class, "100"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    hexEscapes(Boolean .class, "false"),

    public static final PgKey<String> k = new PgKey<>("", String.class, null);

    lc_monetary(String .class);

    public static final PgKey<String> k = new PgKey<>("", String.class, null);


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
