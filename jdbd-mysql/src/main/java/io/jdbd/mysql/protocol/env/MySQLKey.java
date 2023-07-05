package io.jdbd.mysql.protocol.env;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.authentication.MySQLNativePasswordPlugin;
import io.jdbd.mysql.protocol.client.Enums;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.env.OnlyReactor;
import io.jdbd.vendor.env.Redefine;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.ZoneOffset;

public final class MySQLKey<T> {

    /*
     * Properties individually managed after parsing connection string. These property keys are case insensitive.
     */

    /*-------------------below Authentication group-------------------*/


    /**
     * The database user name.
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">user</a>
     */
    public static final MySQLKey<String> USER = new MySQLKey<>("user", String.class, null);

    /**
     * The database user password.
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password</a>
     */
    public static final MySQLKey<String> PASSWORD = new MySQLKey<>("password", String.class, null);

    /**
     * password1
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password1</a>
     * @since MySQL 8.0.28
     */
    public static final MySQLKey<String> PASSWORD1 = new MySQLKey<>("password1", String.class, null);

    /**
     * password2
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password2</a>
     * @since MySQL 8.0.28
     */
    public static final MySQLKey<String> PASSWORD2 = new MySQLKey<>("password2", String.class, null);

    /**
     * password3
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password3</a>
     * @since MySQL 8.0.28
     */
    public static final MySQLKey<String> PASSWORD3 = new MySQLKey<>("password3", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">authenticationPlugins</a>
     */
    public static final MySQLKey<String> AUTHENTICATION_PLUGINS = new MySQLKey<>("authenticationPlugins", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">defaultAuthenticationPlugin</a>
     */
    public static final MySQLKey<String> DEFAULT_AUTHENTICATION_PLUGIN = new MySQLKey<>("defaultAuthenticationPlugin", String.class, MySQLNativePasswordPlugin.PLUGIN_NAME);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">disabledAuthenticationPlugins</a>
     */
    public static final MySQLKey<String> DISABLED_AUTHENTICATION_PLUGINS = new MySQLKey<>("disabledAuthenticationPlugins", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">ociConfigFile</a>
     * @since MySQL 8.0.27
     */
    public static final MySQLKey<String> OCI_CONFIG_FILE = new MySQLKey<>("ociConfigFile", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">ociConfigProfile</a>
     * @since MySQL 8.0.33
     */
    public static final MySQLKey<String> OCI_CONFIG_PROFILE = new MySQLKey<>("ociConfigProfile", String.class, "DEFAULT");

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">authenticationFidoCallbackHandler</a>
     * @since MySQL 8.0.29
     */
    @Deprecated
    public static final MySQLKey<String> AUTHENTICATION_FIDO_CALLBACK_HANDLER = new MySQLKey<>("authenticationFidoCallbackHandler", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">ldapServerHostname</a>
     * @since MySQL 8.0.23
     */
    public static final MySQLKey<String> LDAP_SERVER_HOSTNAME = new MySQLKey<>("ldapServerHostname", String.class, null);

    /*-------------------below Connection group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">connectionAttributes</a>
     */
    public static final MySQLKey<String> CONNECTION_ATTRIBUTES = new MySQLKey<>("connectionAttributes", String.class, null);


    // TODO connectionLifecycleInterceptors


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">useConfigs</a>
     */
    public static final MySQLKey<String> USE_CONFIGS = new MySQLKey<>("useConfigs", String.class, null);

    /**
     * <p>
     * {@link java.util.function.Supplier} function reference. For example : MyClass::clientInoMap
     * <pre><br/>
     *   public static Map&lt;String,String> clientInoMap() {
     *       return Collections.emptyMap();
     *   }
     * </pre>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">clientInfoSupplier</a>
     */
    public static final MySQLKey<String> CLIENT_INFO_SUPPLIER = new MySQLKey<>("clientInfoSupplier", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">createDatabaseIfNotExist</a>
     */
    public static final MySQLKey<Boolean> CREATE_DATABASE_IF_NOT_EXIST = new MySQLKey<>("createDatabaseIfNotExist", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">createDatabaseIfNotExist</a>
     */
    public static final MySQLKey<Boolean> DETECT_CUSTOM_COLLATIONS = new MySQLKey<>("detectCustomCollations", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">disconnectOnExpiredPasswords</a>
     */
    public static final MySQLKey<Boolean> DISCONNECT_ON_EXPIRED_PASSWORDS = new MySQLKey<>("disconnectOnExpiredPasswords", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">interactiveClient</a>
     */
    public static final MySQLKey<Boolean> INTERACTIVE_CLIENT = new MySQLKey<>("interactiveClient", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">passwordCharacterEncoding</a>
     */
    public static final MySQLKey<String> PASSWORD_CHARACTER_ENCODING = new MySQLKey<>("passwordCharacterEncoding", String.class, null);

    /**
     * <p>
     * {@link java.util.function.UnaryOperator} function reference. For example : MyClass::transformProperties
     * <pre><br/>
     *   public static Map&lt;String,String> transformProperties(Map&lt;String,String> map) {
     *       return map;
     *   }
     * </pre>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">propertiesTransform</a>
     */
    public static final MySQLKey<String> PROPERTIES_TRANS_FORM = new MySQLKey<>("propertiesTransform", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">rollbackOnPooledClose</a>
     */
    public static final MySQLKey<Boolean> ROLLBACK_ON_POOLED_CLOSE = new MySQLKey<>("rollbackOnPooledClose", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">useAffectedRows</a>
     */
    public static final MySQLKey<Boolean> USE_AFFECTED_ROWS = new MySQLKey<>("useAffectedRows", Boolean.class, Boolean.FALSE);


    //below  Group

    /**
     * The hostname value from the properties instance passed to the driver.
     */
    public static final MySQLKey<String> HOST = new MySQLKey<>("host", String.class, null);

    /**
     * The port number value from the properties instance passed to the driver.
     */
    public static final MySQLKey<Integer> PORT = new MySQLKey<>("port", Integer.class, 3306);

    /**
     * The communications protocol. Possible values: "tcp" and "pipe".
     */
    public static final MySQLKey<String> PROTOCOL = new MySQLKey<>("protocol", String.class, null);

    /**
     * The name pipes path to use when "protocol=pipe'.
     */
    public static final MySQLKey<String> PATH = new MySQLKey<>("path", String.class, "namedPipePath");

    /**
     * The server type in a replication setup. Possible values: "master" and "slave".
     */
    public static final MySQLKey<String> TYPE = new MySQLKey<>("type", String.class, null);

    /**
     * The address value ("host:port") from the properties instance passed to the driver.
     */
    public static final MySQLKey<String> ADDRESS = new MySQLKey<>("address", String.class, null);

    /**
     * The host priority in a list of hosts.
     */
    public static final MySQLKey<String> PRIORITY = new MySQLKey<>("priority", String.class, null);

    /**
     * The database value from the properties instance passed to the driver.
     */
    public static final MySQLKey<String> DB_NAME = new MySQLKey<>("dbname", String.class, null);
    // blow Connection Group


    detectCustomCollations("false",Boolean .class), //

    disabledAuthenticationPlugins(null,String .class), //

    disconnectOnExpiredPasswords("true",Boolean .class), //

    interactiveClient("false",Boolean .class), //

    ldapServerHostname(String .class), //

    passwordCharacterEncoding(Charset .class), //

    propertiesTransform(Class .class), //

    rollbackOnPooledClose("true",Boolean .class), //

    useAffectedRows("false",Boolean .class), //


    // blow Session Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html
    sessionVariables(String .class), //

    characterEncoding(Charset .class), //

    characterSetResults(String .class), //

    connectionCollation(String .class), //

    customCharsetMapping(String .class),

    // blow Networking Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html
    socksProxyHost(String .class), //

    socksProxyPort("1080",Integer .class), //

    @Redefine
    socketFactory(null,Class .class), //

    connectTimeout("0",Long .class), //

    socketTimeout("0",Long .class), //

    dnsSrv("false",Boolean .class), //

    localSocketAddress(null,String .class), //

    maxAllowedPacket("maxAllowedPacket",Integer.toString(1<<26),Integer.class), //

    tcpKeepAlive("true",Boolean .class), //

    tcpNoDelay("true",Boolean .class), //

    tcpRcvBuf("0",Integer .class), //

    tcpSndBuf("0",Integer .class), //

    tcpTrafficClass("0",Integer .class), //

    useCompression("false",Boolean .class), //

    useUnbufferedInput("true",Boolean .class), //

    // blow Security Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html
    paranoid("false",Boolean .class, false), //

    serverRSAPublicKeyFile(null,Path .class), //

    allowPublicKeyRetrieval("false",Boolean .class), //

    sslMode("PREFERRED",Enums.SslMode .class), //

    trustCertificateKeyStoreUrl(null,Path .class), //

    trustCertificateKeyStoreType("JKS",String .class), //

    trustCertificateKeyStorePassword(null,String .class), //

    fallbackToSystemTrustStore("true",Boolean .class), //

    clientCertificateKeyStoreUrl(null,Path .class), //

    clientCertificateKeyStoreType("JKS",String .class), //

    clientCertificateKeyStorePassword(null,String .class), //

    fallbackToSystemKeyStore("true",Boolean .class), //

    enabledSSLCipherSuites(null,String .class), //

    enabledTLSProtocols(null,String .class), //

    allowLoadLocalInfile("false",Boolean .class), //

    allowLoadLocalInfileInPath(null,Path .class), //

    /**
     * @deprecated jdbd always allow.
     */
    @Deprecated
    allowMultiQueries("false",Boolean .class), //

    allowUrlInLocalInfile("false",Boolean .class), //

    @Deprecated
    requireSSL("false",Boolean .class), //

    @Deprecated
    useSSL("true",Boolean .class), //

    @Deprecated
    verifyServerCertificate("false",Boolean .class), //

    //below Statements Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-statements.html

    cacheDefaultTimezone("true",Boolean .class), //

    continueBatchOnError("true",Boolean .class), //

    dontTrackOpenResources("false",Boolean .class), //

    queryInterceptors(null,String .class), //

    queryTimeoutKillsConnection("false",Boolean .class), //

    //below  Prepared Statements https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html
    allowNanAndInf("false",Boolean .class), //

    autoClosePStmtStreams("false",Boolean .class), //

    compensateOnDuplicateKeyUpdateCounts("false",Boolean .class), //

    emulateUnsupportedPstmts("true",Boolean .class), //

    generateSimpleParameterMetadata("false",Boolean .class), //

    processEscapeCodesForPrepStmts("true",Boolean .class), //

    useServerPrepStmts("false",Boolean .class), //

    useStreamLengthsInPrepStmts("true",Boolean .class), //

    //below  Result Sets https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html
    clobberStreamingResults("false",Boolean .class), //

    emptyStringsConvertToZero("true",Boolean .class), //

    holdResultsOpenOverStatementClose("false",Boolean .class), //

    jdbcCompliantTruncation("true",Boolean .class), //

    maxRows("-1",Integer .class), //

    netTimeoutForStreamingResults("600",Integer .class), //

    padCharsWithSpace("false",Boolean .class), //

    populateInsertRowWithDefaultValues("false",Boolean .class), //

    strictUpdates("true",Boolean .class), //

    @Deprecated
    tinyInt1isBit("true",Boolean .class), //

    transformedBitIsBoolean("false",Boolean .class), //

    //below Metadata Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html

    getProceduresReturnsFunctions("true",Boolean .class), //

    noAccessToProcedureBodies("false",Boolean .class), //

    nullDatabaseMeansCurrent("nullDatabaseMeansCurrent","nullCatalogMeansCurrent","false",Boolean .class), //

    useHostsInPrivileges("true",Boolean .class), //

    useInformationSchema("false",Boolean .class), //

    //below BLOB/CLOB processing https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html
    autoDeserialize("false",Boolean .class), //

    blobSendChunkSize(Integer.toString(0xFF_FF_FE),Integer.class), //

    @Deprecated
    blobsAreStrings("false",Boolean .class), //

    clobCharacterEncoding(null,Charset .class), //

    emulateLocators("false",Boolean .class), //

    functionsNeverReturnBlobs("false",Boolean .class), //

    locatorFetchBufferSize("1048576",Integer .class), //

    //below Datetime types processing  https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-datetime-types-processing.html
    connectionTimeZone(Constants.LOCAL, ZoneOffset .class),

    forceConnectionTimeZoneToSession("false",Boolean .class), //

    noDatetimeStringSync("false",Boolean .class), //

    preserveInstants("true",Boolean .class), //

    sendFractionalSeconds("true",Boolean .class), //

    sendFractionalSecondsForTime("true",Boolean .class), //

    treatUtilDateAsTimestamp("true",Boolean .class), //

    yearIsDateType("true",Boolean .class), //

    zeroDateTimeBehavior("EXCEPTION",Enums.ZeroDatetimeBehavior .class), //

    //below High Availability and Clustering https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html

    autoReconnect("false",Boolean .class), //

    autoReconnectForPools("false",Boolean .class), //

    failOverReadOnly("true",Boolean .class), //

    maxReconnects("3",Integer .class), //

    reconnectAtTxEnd("false",Boolean .class), //

    retriesAllDown("120",Integer .class), //

    initialTimeout("2",Integer .class), //

    queriesBeforeRetrySource("50",Integer .class), //

    secondsBeforeRetrySource("30",Integer .class), //

    allowReplicaDownConnections("false",Boolean .class), //

    allowSourceDownConnections("false",Boolean .class), //

    ha_enableJMX("ha.enableJMX","haEnableJMX","false",Boolean .class), //

    loadBalanceHostRemovalGracePeriod("15000",Integer .class), //

    readFromSourceWhenNoReplicas("readFromSourceWhenNoReplicas","readFromMasterWhenNoSlaves","false",Boolean .class), //

    selfDestructOnPingMaxOperations("0",Integer .class), //

    selfDestructOnPingSecondsLifetime("0",Integer .class), //

    ha_loadBalanceStrategy("ha.loadBalanceStrategy","haLoadBalanceStrategy","random",String .class), //

    loadBalanceAutoCommitStatementRegex(null,String .class), //

    loadBalanceAutoCommitStatementThreshold("0",Integer .class), //

    loadBalanceBlocklistTimeout("loadBalanceBlocklistTimeout","loadBalanceBlacklistTimeout","0",Integer .class),

    loadBalanceConnectionGroup(null,String .class), //

    loadBalanceExceptionChecker("com.mysql.cj.jdbc.ha.StandardLoadBalanceExceptionChecker",Class .class), //

    loadBalancePingTimeout("0",Long .class), //

    loadBalanceSQLExceptionSubclassFailover(null,String .class), //

    loadBalanceSQLStateFailover(null,String .class), //

    loadBalanceValidateConnectionOnSwapServer("false",Boolean .class), //

    pinGlobalTxToPhysicalConnection("false",Boolean .class), //

    replicationConnectionGroup(null,String .class), //

    resourceId(null,String .class), //

    serverAffinityOrder(null,String .class), //

    //below Performance Extensions
    callableStmtCacheSize("100",Integer .class), //

    metadataCacheSize("50",Integer .class), //

    useLocalSessionState("false",Boolean .class), //

    useLocalTransactionState("false",Boolean .class), //

    prepStmtCacheSize("25",Integer .class), //

    prepStmtCacheSqlLimit("256",Integer .class), //

    parseInfoCacheFactory("com.mysql.cj.PerConnectionLRUFactory",Class .class), //

    serverConfigCacheFactory("com.mysql.cj.util.PerVmServerConfigCacheFactory",Class .class), //

    alwaysSendSetIsolation("true",Boolean .class), //

    maintainTimeStats("true",Boolean .class), //

    useCursorFetch("false",Boolean .class), //

    cacheCallableStmts("false",Boolean .class), //

    cachePrepStmts("false",Boolean .class), //

    cacheResultSetMetadata("false",Boolean .class), //

    cacheServerConfiguration("false",Boolean .class), //

    defaultFetchSize("0",Boolean .class), //

    dontCheckOnDuplicateKeyUpdateInSQL("false",Boolean .class), //

    elideSetAutoCommits("false",Boolean .class), //

    enableEscapeProcessing("true",Boolean .class), //

    enableQueryTimeouts("true",Boolean .class), //

    largeRowSizeThreshold("2048",Integer .class), //

    readOnlyPropagatesToServer("true",Boolean .class), //

    rewriteBatchedStatements("false",Boolean .class), //

    useReadAheadInput("true",Boolean .class), //

    //below  Debugging/Profiling https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-debugging-profiling.html
    logger("com.mysql.cj.log.StandardLogger",Class .class), //

    profilerEventHandler("com.mysql.cj.log.LoggingProfilerEventHandler",Class .class), //

    useNanosForElapsedTime("false",Boolean .class), //

    maxQuerySizeToLog("2048",Integer .class), //

    profileSQL("false",Boolean .class), //

    logSlowQueries("false",Boolean .class), //

    slowQueryThresholdMillis("2000",Long .class), //

    slowQueryThresholdNanos("0",Long .class), //

    autoSlowLog("true",Boolean .class), //

    explainSlowQueries("false",Boolean .class), //

    gatherPerfMetrics("false",Boolean .class), //

    reportMetricsIntervalMillis("30000",Long .class), //

    logXaCommands("false",Boolean .class), //

    traceProtocol("false",Boolean .class), //

    enablePacketDebug("false",Boolean .class), //

    packetDebugBufferSize("20",Integer .class), //

    useUsageAdvisor("false",Boolean .class), //

    resultSetSizeThreshold("100",Integer .class), //

    autoGenerateTestcaseScript("false",Boolean .class), //

    //below  Exceptions/Warnings https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-exceptions-warnings.html
    dumpQueriesOnException("false",Boolean .class), //

    exceptionInterceptors(null,String .class), //

    ignoreNonTxTables("false",Boolean .class), //

    includeInnodbStatusInDeadlockExceptions("false",Boolean .class), //

    includeThreadDumpInDeadlockExceptions("false",Boolean .class), //

    includeThreadNamesAsStatementComment("false",Boolean .class), //

    useOnlyServerErrorMessages("true",Boolean .class), //

    //below Tunes for integration with other products https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-tunes-for-integration-with-other-products.html
    overrideSupportsIntegrityEnhancementFacility("false",Boolean .class), //

    ultraDevHack("false",Boolean .class), //

    //below JDBC compliance https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-jdbc-compliance.html
    useColumnNamesInFindColumn("false",Boolean .class), //

    pedantic("false",Boolean .class), //

    useOldAliasMetadataBehavior("false",Boolean .class), //

    //below X Protocol and X DevAPI https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-x-protocol-and-x-devapi.html
    xdevapiAuth("xdevapi.auth","xdevapiAuth","PLAIN",String .class), //

    xdevapiCompression("xdevapi.compression","xdevapiCompression","PREFERRED",Enums.Compression .class), //

    xdevapiCompressionAlgorithm("xdevapi.compression-algorithm","xdevapiCompressionAlgorithm",null,String .class), //

    xdevapiCompressionExtensions("xdevapi.compression-extensions","xdevapiCompressionExtensions",null,String .class), //

    xdevapiConnectTimeout("xdevapi.connect-timeout","xdevapiConnectTimeout","10000",Long .class), //

    xdevapiConnectionAttributes("xdevapi.connection-attributes","xdevapiConnectionAttributes",null,String .class), //

    xdevapiDnsSrv("xdevapi.dns-srv","xdevapiDnsSrv","false",Boolean .class), //

    xdevapiFallbackToSystemKeyStore("xdevapi.fallback-to-system-keystore","xdevapiFallbackToSystemKeyStore","true",Boolean .class), //

    xdevapiFallbackToSystemTrustStore("xdevapi.fallback-to-system-truststore","xdevapiFallbackToSystemTrustStore","true",Boolean .class), //

    xdevapiSslKeyStoreUrl("xdevapi.ssl-keystore","xdevapiSslKeystore",null,String .class), //

    xdevapiSslKeyStorePassword("xdevapi.ssl-keystore-password","xdevapiSslKeystorePassword",null,String .class), //

    xdevapiSslKeyStoreType("xdevapi.ssl-keystore-type","xdevapiSslKeystoreType","JKS",String .class), //

    xdevapiSslMode("xdevapi.ssl-mode","xdevapiSslMode","",Enums.XdevapiSslMode .class), //

    xdevapiSSLTrustStoreUrl("xdevapi.ssl-truststore","xdevapiSSLTruststore",null,String .class), //

    xdevapiSSLTrustStorePassword("xdevapi.ssl-truststore-password","xdevapiSSLTruststorePassword",null,String .class), //

    xdevapiSSLTrustStoreType("xdevapi.ssl-truststore-type","xdevapiSSLTruststoreType","JKS",String .class), //

    xdevapiTlsCiphersuites("xdevapi.tls-ciphersuites","xdevapiTlsCiphersuites",null,String .class), //

    xdevapiTlsVersions("xdevapi.tls-versions","xdevapiTlsVersions",null,String .class), //


    //below unknown
    allowMasterDownConnections("allowMasterDownConnections","false",Boolean .class), //

    allowSlaveDownConnections("allowSlaveDownConnections","false",Boolean .class), //

    loadBalanceBlacklistTimeout("loadBalanceBlacklistTimeout","0",Integer .class), //

    queriesBeforeRetryMaster("queriesBeforeRetryMaster","50",Integer .class), //

    readFromMasterWhenNoSlaves("readFromMasterWhenNoSlaves","false",Boolean .class), //

    secondsBeforeRetryMaster("secondsBeforeRetryMaster","30",Object .class), //

    xdevapiAsyncResponseTimeout("xdevapi.asyncResponseTimeout","xdevapiAsyncResponseTimeout",null,Object .class), //

    xdevapiUseAsyncProtocol("xdevapi.useAsyncProtocol","xdevapiUseAsyncProtocol",null,Object .class), //

    @OnlyReactor
    timeTruncateFractional("timeTruncateFractional","true",Boolean .class),

    @OnlyReactor
    clientPrepareSupportStream("false",Boolean .class),

    factoryWorkerCount("50",Integer .class);


    public final String name;

    public final String alias;

    public final Class<T> valueClass;

    public final T defaultValue;

    private MySQLKey(String name, Class<T> valueClass, @Nullable T defaultValue) {
        this(name, null, valueClass, defaultValue);
    }

    private MySQLKey(String name, String alias, Class<T> valueClass, @Nullable T defaultValue) {
        this.name = name;
        this.alias = alias;
        this.valueClass = valueClass;
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(MySQLKey.class.getName())
                .append("[ name : ")
                .append(this.name)
                .append(" , alias : ")
                .append(this.alias)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" , valueClass : ")
                .append(this.valueClass.getName())
                .append(" , defaultValue : ")
                .append(this.defaultValue)
                .append(" ]")
                .toString();
    }


}
