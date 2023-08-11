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

    public static final PgKey<String> BINARY_TRANSFER_ENABLE = new PgKey<>("binaryTransferEnable", String.class, null);

    public static final PgKey<String> BINARY_TRANSFER_DISABLE = new PgKey<>("binaryTransferDisable", String.class, null);

    public static final PgKey<Integer> DATABASE_METADATA_CACHE_FIELDS = new PgKey<>("databaseMetadataCacheFields", Integer.class, 65536);

    public static final PgKey<Integer> DATABASE_METADATA_CACHE_FIELDS_MIB = new PgKey<>("databaseMetadataCacheFieldsMiB", Integer.class, 5);

    public static final PgKey<Integer> PREPARE_THRESHOLD = new PgKey<>("prepareThreshold", Integer.class, null);

    public static final PgKey<Integer> PREPARED_STATEMENT_CACHE_QUERIES = new PgKey<>("preparedStatementCacheQueries", Integer.class, null);

    public static final PgKey<Integer> PREPARED_STATEMENT_CACHE_SIZE_MIB = new PgKey<>("preparedStatementCacheSizeMiB", Integer.class, null);

    public static final PgKey<String> PREFER_QUERY_MODE = new PgKey<>("preferQueryMode", String.class, null);

    public static final PgKey<Integer> DEFAULT_ROW_FETCH_SIZE = new PgKey<>("defaultRowFetchSize", Integer.class, 0);

    public static final PgKey<Integer> LOGIN_TIMEOUT = new PgKey<>("loginTimeout", Integer.class, 0);

    public static final PgKey<Integer> CONNECT_TIMEOUT = new PgKey<>("connectTimeout", Integer.class, 0);

    public static final PgKey<Integer> SOCKET_TIMEOUT = new PgKey<>("socketTimeout", Integer.class, null);

    public static final PgKey<Integer> CANCEL_SIGNAL_TIMEOUT = new PgKey<>("cancelSignalTimeout", Integer.class, 10);


    public static final PgKey<Boolean> TCP_KEEP_ALIVE = new PgKey<>("tcpKeepAlive", Boolean.class, Boolean.FALSE);


    public static final PgKey<Integer> unknownLength = new PgKey<>("unknownLength", Integer.class, Integer.MAX_VALUE);


    public static final PgKey<String> STRING_TYPE = new PgKey<>("stringType", String.class, null);


    public static final PgKey<String> APPLICATION_NAME = new PgKey<>("applicationName", String.class, null);


    public static final PgKey<String> KERBEROS_SERVER_NAME = new PgKey<>("kerberosServerName", String.class, null);

    public static final PgKey<String> JAAS_APPLICATION_NAME = new PgKey<>("jaasApplicationName", String.class, null);


    public static final PgKey<Boolean> JAAS_LOGIN = new PgKey<>("jaasLogin", Boolean.class, Boolean.TRUE);


    public static final PgKey<Enums.GSSEncMode> GSS_ENC_MODE = new PgKey<>("gssEncMode", Enums.GSSEncMode.class, Enums.GSSEncMode.DISABLE);


    public static final PgKey<String> GSS_LIB = new PgKey<>("gssLib", String.class, "auto");


    public static final PgKey<String> SSPI_SERVICE_CLASS = new PgKey<>("sspiServiceClass", String.class, "POSTGRES");


    public static final PgKey<Boolean> USE_SPNEGO = new PgKey<>("useSpnego", Boolean.class, Boolean.FALSE);


    public static final PgKey<Boolean> READ_ONLY = new PgKey<>("readOnly", Boolean.class, Boolean.FALSE);


    public static final PgKey<String> READ_ONLY_MODE = new PgKey<>("readOnlyMode", String.class, null);


    public static final PgKey<Boolean> DISABLE_COLUMN_SANITISER = new PgKey<>("disableColumnSanitiser", Boolean.class, Boolean.FALSE);

    public static final PgKey<String> ASSUME_MIN_SERVER_VERSION = new PgKey<>("assumeMinServerVersion", String.class, null);


    public static final PgKey<String> CURRENT_SCHEMA = new PgKey<>("currentSchema", String.class, null);


    public static final PgKey<String> TARGET_SERVER_TYPE = new PgKey<>("targetServerType", String.class, null);


    public static final PgKey<Integer> HOST_RECHECK_SECONDS = new PgKey<>("hostRecheckSeconds", Integer.class, 10);


    public static final PgKey<Boolean> LOAD_BALANCE_HOSTS = new PgKey<>("loadBalanceHosts", Boolean.class, Boolean.FALSE);

    public static final PgKey<String> REPLICATION = new PgKey<>("replication", String.class, "false"); // This parameter accepts two values; true and database .

    public static final PgKey<String> ESCAPE_SYNTAX_CALL_MODE = new PgKey<>("escapeSyntaxCallMode", String.class, null);


    public static final PgKey<Boolean> LOG_SERVER_ERROR_DETAIL = new PgKey<>("logServerErrorDetail", Boolean.class, Boolean.TRUE);


    public static final PgKey<String> KEY_STORE_TYPE = new PgKey<>("keyStoreType", String.class, null);

    public static final PgKey<String> KEY_STORE_URL = new PgKey<>("keyStoreUrl", String.class, null);

    public static final PgKey<String> KEY_STORE_PASSWORD = new PgKey<>("keyStorePassword", String.class, null);

    public static final PgKey<String> TRUST_STORE_TYPE = new PgKey<>("trustStoreType", String.class, null);

    public static final PgKey<String> TRUST_STORE_URL = new PgKey<>("trustStoreUrl", String.class, null);

    public static final PgKey<String> TRUST_STORE_PASSWORD = new PgKey<>("trustStorePassword", String.class, null);

    public static final PgKey<Boolean> FALLBACK_TO_SYSTEM_KEY_STORE = new PgKey<>("fallbackToSystemKeyStore", Boolean.class, Boolean.FALSE);


    public static final PgKey<Integer> FACTORY_WORKER_COUNT = new PgKey<>("factoryWorkerCount", Integer.class, null);


    public static final PgKey<Boolean> HEX_ESCAPES = new PgKey<>("hexEscapes", Boolean.class, Boolean.FALSE);


    public static final PgKey<String> LC_MONETARY = new PgKey<>("lc_monetary", String.class, null);





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
