package io.jdbd.mysql.protocol.conf;

import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.authentication.MySQLNativePasswordPlugin;
import io.jdbd.mysql.protocol.client.Enums;
import io.jdbd.vendor.conf.OnlyReactor;
import io.jdbd.vendor.conf.PropertyKey;
import io.jdbd.vendor.conf.Redefine;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * PropertyKey handles connection property names, their camel-case aliases and case sensitivity.
 *
 * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html">Configuration Properties</a>
 */
public enum MyKey implements PropertyKey {
    /*
     * Properties individually managed after parsing connection string. These property keys are case insensitive.
     */
    //below Authentication Group
    /** The database user name. */
    user(String.class, false),
    /** The database user password. */
    password(String.class, false),

    //below  Group
    /** The hostname value from the properties instance passed to the driver. */
    host(String.class, false),
    /** The port number value from the properties instance passed to the driver. */
    port(Integer.class, false),

    /** The communications protocol. Possible values: "tcp" and "pipe". */
    protocol(String.class, false),
    /** The name pipes path to use when "protocol=pipe'. */
    path("namedPipePath", String.class, false),
    /** The server type in a replication setup. Possible values: "master" and "slave". */
    type(String.class, false),
    /** The address value ("host:port") from the properties instance passed to the driver. */
    address(String.class, false),

    /** The host priority in a list of hosts. */
    priority(String.class, false),
    /** The database value from the properties instance passed to the driver. */
    dbname(String.class, false), //

    // blow Connection Group
    connectionAttributes(String.class), //
    @Deprecated
    connectionLifecycleInterceptors(Class.class), //
    useConfigs(String.class), //
    authenticationPlugins(String.class), //

    @Deprecated
    clientInfoProvider("com.mysql.cj.jdbc.CommentClientInfoProvider", Class.class), //
    createDatabaseIfNotExist("false", Boolean.class), //
    /** @deprecated always CATALOG in jdbd */
    @Deprecated
    databaseTerm("CATALOG", String.class), //
    defaultAuthenticationPlugin(MySQLNativePasswordPlugin.PLUGIN_NAME, String.class), //

    detectCustomCollations("false", Boolean.class), //
    disabledAuthenticationPlugins(null, String.class), //
    disconnectOnExpiredPasswords("true", Boolean.class), //
    interactiveClient("false", Boolean.class), //

    ldapServerHostname(String.class), //
    passwordCharacterEncoding(Charset.class), //
    propertiesTransform(Class.class), //
    rollbackOnPooledClose("true", Boolean.class), //

    useAffectedRows("false", Boolean.class), //


    // blow Session Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html
    sessionVariables(String.class), //
    characterEncoding(Charset.class), //
    characterSetResults(String.class), //
    connectionCollation(String.class), //
    customCharsetMapping(String.class),

    // blow Networking Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html
    socksProxyHost(String.class), //
    socksProxyPort("1080", Integer.class), //
    @Redefine
    socketFactory(null, Class.class), //
    connectTimeout("0", Long.class), //

    socketTimeout("0", Long.class), //
    dnsSrv("false", Boolean.class), //
    localSocketAddress(null, String.class), //
    maxAllowedPacket("maxAllowedPacket", Integer.toString(1 << 26), Integer.class), //

    tcpKeepAlive("true", Boolean.class), //
    tcpNoDelay("true", Boolean.class), //
    tcpRcvBuf("0", Integer.class), //
    tcpSndBuf("0", Integer.class), //

    tcpTrafficClass("0", Integer.class), //
    useCompression("false", Boolean.class), //
    useUnbufferedInput("true", Boolean.class), //

    // blow Security Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html
    paranoid("false", Boolean.class, false), //
    serverRSAPublicKeyFile(null, Path.class), //
    allowPublicKeyRetrieval("false", Boolean.class), //
    sslMode("PREFERRED", Enums.SslMode.class), //

    trustCertificateKeyStoreUrl(null, Path.class), //
    trustCertificateKeyStoreType("JKS", String.class), //
    trustCertificateKeyStorePassword(null, String.class), //
    fallbackToSystemTrustStore("true", Boolean.class), //

    clientCertificateKeyStoreUrl(null, Path.class), //
    clientCertificateKeyStoreType("JKS", String.class), //
    clientCertificateKeyStorePassword(null, String.class), //
    fallbackToSystemKeyStore("true", Boolean.class), //

    enabledSSLCipherSuites(null, String.class), //
    enabledTLSProtocols(null, String.class), //
    allowLoadLocalInfile("false", Boolean.class), //
    allowLoadLocalInfileInPath(null, Path.class), //

    /**
     * @deprecated jdbd always allow.
     */
    @Deprecated
    allowMultiQueries("false", Boolean.class), //
    allowUrlInLocalInfile("false", Boolean.class), //
    @Deprecated
    requireSSL("false", Boolean.class), //
    @Deprecated
    useSSL("true", Boolean.class), //

    @Deprecated
    verifyServerCertificate("false", Boolean.class), //

    //below Statements Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-statements.html

    cacheDefaultTimezone("true", Boolean.class), //
    continueBatchOnError("true", Boolean.class), //
    dontTrackOpenResources("false", Boolean.class), //
    queryInterceptors(null, String.class), //
    queryTimeoutKillsConnection("false", Boolean.class), //

    //below  Prepared Statements https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html
    allowNanAndInf("false", Boolean.class), //
    autoClosePStmtStreams("false", Boolean.class), //
    compensateOnDuplicateKeyUpdateCounts("false", Boolean.class), //
    emulateUnsupportedPstmts("true", Boolean.class), //

    generateSimpleParameterMetadata("false", Boolean.class), //
    processEscapeCodesForPrepStmts("true", Boolean.class), //
    useServerPrepStmts("false", Boolean.class), //
    useStreamLengthsInPrepStmts("true", Boolean.class), //

    //below  Result Sets https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html
    clobberStreamingResults("false", Boolean.class), //
    emptyStringsConvertToZero("true", Boolean.class), //
    holdResultsOpenOverStatementClose("false", Boolean.class), //
    jdbcCompliantTruncation("true", Boolean.class), //

    maxRows("-1", Integer.class), //
    netTimeoutForStreamingResults("600", Integer.class), //
    padCharsWithSpace("false", Boolean.class), //
    populateInsertRowWithDefaultValues("false", Boolean.class), //

    strictUpdates("true", Boolean.class), //
    tinyInt1isBit("true", Boolean.class), //
    transformedBitIsBoolean("false", Boolean.class), //

    //below Metadata Group https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html

    getProceduresReturnsFunctions("true", Boolean.class), //
    noAccessToProcedureBodies("false", Boolean.class), //
    nullDatabaseMeansCurrent("nullDatabaseMeansCurrent", "nullCatalogMeansCurrent", "false", Boolean.class), //
    useHostsInPrivileges("true", Boolean.class), //

    useInformationSchema("false", Boolean.class), //

    //below BLOB/CLOB processing https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html
    autoDeserialize("false", Boolean.class), //
    blobSendChunkSize("1048576", Integer.class), //
    blobsAreStrings("false", Boolean.class), //
    clobCharacterEncoding(null, Charset.class), //

    emulateLocators("false", Boolean.class), //
    functionsNeverReturnBlobs("false", Boolean.class), //
    locatorFetchBufferSize("1048576", Integer.class), //

    //below Datetime types processing  https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-datetime-types-processing.html
    connectionTimeZone(Constants.LOCAL, ZoneOffset.class),
    forceConnectionTimeZoneToSession("false", Boolean.class), //
    noDatetimeStringSync("false", Boolean.class), //
    preserveInstants("true", Boolean.class), //

    sendFractionalSeconds("true", Boolean.class), //
    sendFractionalSecondsForTime("true", Boolean.class), //
    treatUtilDateAsTimestamp("true", Boolean.class), //
    yearIsDateType("true", Boolean.class), //

    zeroDateTimeBehavior("EXCEPTION", Enums.ZeroDatetimeBehavior.class), //

    //below High Availability and Clustering https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html

    autoReconnect("false", Boolean.class), //
    autoReconnectForPools("false", Boolean.class), //
    failOverReadOnly("true", Boolean.class), //
    maxReconnects("3", Integer.class), //

    reconnectAtTxEnd("false", Boolean.class), //
    retriesAllDown("120", Integer.class), //
    initialTimeout("2", Integer.class), //
    queriesBeforeRetrySource("50", Integer.class), //

    secondsBeforeRetrySource("30", Integer.class), //
    allowReplicaDownConnections("false", Boolean.class), //
    allowSourceDownConnections("false", Boolean.class), //
    ha_enableJMX("ha.enableJMX", "haEnableJMX", "false", Boolean.class), //

    loadBalanceHostRemovalGracePeriod("15000", Integer.class), //
    readFromSourceWhenNoReplicas("readFromSourceWhenNoReplicas", "readFromMasterWhenNoSlaves", "false", Boolean.class), //
    selfDestructOnPingMaxOperations("0", Integer.class), //
    selfDestructOnPingSecondsLifetime("0", Integer.class), //

    ha_loadBalanceStrategy("ha.loadBalanceStrategy", "haLoadBalanceStrategy", "random", String.class), //
    loadBalanceAutoCommitStatementRegex(null, String.class), //
    loadBalanceAutoCommitStatementThreshold("0", Integer.class), //
    loadBalanceBlocklistTimeout("loadBalanceBlocklistTimeout", "loadBalanceBlacklistTimeout", "0", Integer.class),

    loadBalanceConnectionGroup(null, String.class), //
    loadBalanceExceptionChecker("com.mysql.cj.jdbc.ha.StandardLoadBalanceExceptionChecker", Class.class), //
    loadBalancePingTimeout("0", Long.class), //
    loadBalanceSQLExceptionSubclassFailover(null, String.class), //

    loadBalanceSQLStateFailover(null, String.class), //
    loadBalanceValidateConnectionOnSwapServer("false", Boolean.class), //
    pinGlobalTxToPhysicalConnection("false", Boolean.class), //
    replicationConnectionGroup(null, String.class), //

    resourceId(null, String.class), //
    serverAffinityOrder(null, String.class), //

    //below Performance Extensions
    callableStmtCacheSize("100", Integer.class), //
    metadataCacheSize("50", Integer.class), //
    useLocalSessionState("false", Boolean.class), //
    useLocalTransactionState("false", Boolean.class), //

    prepStmtCacheSize("25", Integer.class), //
    prepStmtCacheSqlLimit("256", Integer.class), //
    parseInfoCacheFactory("com.mysql.cj.PerConnectionLRUFactory", Class.class), //
    serverConfigCacheFactory("com.mysql.cj.util.PerVmServerConfigCacheFactory", Class.class), //

    alwaysSendSetIsolation("true", Boolean.class), //
    maintainTimeStats("true", Boolean.class), //
    useCursorFetch("false", Boolean.class), //
    cacheCallableStmts("false", Boolean.class), //

    cachePrepStmts("false", Boolean.class), //
    cacheResultSetMetadata("false", Boolean.class), //
    cacheServerConfiguration("false", Boolean.class), //
    defaultFetchSize("0", Boolean.class), //

    dontCheckOnDuplicateKeyUpdateInSQL("false", Boolean.class), //
    elideSetAutoCommits("false", Boolean.class), //
    enableEscapeProcessing("true", Boolean.class), //
    enableQueryTimeouts("true", Boolean.class), //

    largeRowSizeThreshold("2048", Integer.class), //
    readOnlyPropagatesToServer("true", Boolean.class), //
    rewriteBatchedStatements("false", Boolean.class), //
    useReadAheadInput("true", Boolean.class), //

    //below  Debugging/Profiling https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-debugging-profiling.html
    logger("com.mysql.cj.log.StandardLogger", Class.class), //
    profilerEventHandler("com.mysql.cj.log.LoggingProfilerEventHandler", Class.class), //
    useNanosForElapsedTime("false", Boolean.class), //
    maxQuerySizeToLog("2048", Integer.class), //

    profileSQL("false", Boolean.class), //
    logSlowQueries("false", Boolean.class), //
    slowQueryThresholdMillis("2000", Long.class), //
    slowQueryThresholdNanos("0", Long.class), //

    autoSlowLog("true", Boolean.class), //
    explainSlowQueries("false", Boolean.class), //
    gatherPerfMetrics("false", Boolean.class), //
    reportMetricsIntervalMillis("30000", Long.class), //

    logXaCommands("false", Boolean.class), //
    traceProtocol("false", Boolean.class), //
    enablePacketDebug("false", Boolean.class), //
    packetDebugBufferSize("20", Integer.class), //

    useUsageAdvisor("false", Boolean.class), //
    resultSetSizeThreshold("100", Integer.class), //
    autoGenerateTestcaseScript("false", Boolean.class), //

    //below  Exceptions/Warnings https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-exceptions-warnings.html
    dumpQueriesOnException("false", Boolean.class), //
    exceptionInterceptors(null, String.class), //
    ignoreNonTxTables("false", Boolean.class), //
    includeInnodbStatusInDeadlockExceptions("false", Boolean.class), //

    includeThreadDumpInDeadlockExceptions("false", Boolean.class), //
    includeThreadNamesAsStatementComment("false", Boolean.class), //
    useOnlyServerErrorMessages("true", Boolean.class), //

    //below Tunes for integration with other products https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-tunes-for-integration-with-other-products.html
    overrideSupportsIntegrityEnhancementFacility("false", Boolean.class), //
    ultraDevHack("false", Boolean.class), //

    //below JDBC compliance https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-jdbc-compliance.html
    useColumnNamesInFindColumn("false", Boolean.class), //
    pedantic("false", Boolean.class), //
    useOldAliasMetadataBehavior("false", Boolean.class), //

    //below X Protocol and X DevAPI https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-x-protocol-and-x-devapi.html
    xdevapiAuth("xdevapi.auth", "xdevapiAuth", "PLAIN", String.class), //
    xdevapiCompression("xdevapi.compression", "xdevapiCompression", "PREFERRED", Enums.Compression.class), //
    xdevapiCompressionAlgorithm("xdevapi.compression-algorithm", "xdevapiCompressionAlgorithm", null, String.class), //
    xdevapiCompressionExtensions("xdevapi.compression-extensions", "xdevapiCompressionExtensions", null, String.class), //

    xdevapiConnectTimeout("xdevapi.connect-timeout", "xdevapiConnectTimeout", "10000", Long.class), //
    xdevapiConnectionAttributes("xdevapi.connection-attributes", "xdevapiConnectionAttributes", null, String.class), //
    xdevapiDnsSrv("xdevapi.dns-srv", "xdevapiDnsSrv", "false", Boolean.class), //
    xdevapiFallbackToSystemKeyStore("xdevapi.fallback-to-system-keystore", "xdevapiFallbackToSystemKeyStore", "true", Boolean.class), //

    xdevapiFallbackToSystemTrustStore("xdevapi.fallback-to-system-truststore", "xdevapiFallbackToSystemTrustStore", "true", Boolean.class), //
    xdevapiSslKeyStoreUrl("xdevapi.ssl-keystore", "xdevapiSslKeystore", null, String.class), //
    xdevapiSslKeyStorePassword("xdevapi.ssl-keystore-password", "xdevapiSslKeystorePassword", null, String.class), //
    xdevapiSslKeyStoreType("xdevapi.ssl-keystore-type", "xdevapiSslKeystoreType", "JKS", String.class), //

    xdevapiSslMode("xdevapi.ssl-mode", "xdevapiSslMode", "", Enums.XdevapiSslMode.class), //
    xdevapiSSLTrustStoreUrl("xdevapi.ssl-truststore", "xdevapiSSLTruststore", null, String.class), //
    xdevapiSSLTrustStorePassword("xdevapi.ssl-truststore-password", "xdevapiSSLTruststorePassword", null, String.class), //
    xdevapiSSLTrustStoreType("xdevapi.ssl-truststore-type", "xdevapiSSLTruststoreType", "JKS", String.class), //

    xdevapiTlsCiphersuites("xdevapi.tls-ciphersuites", "xdevapiTlsCiphersuites", null, String.class), //
    xdevapiTlsVersions("xdevapi.tls-versions", "xdevapiTlsVersions", null, String.class), //


    //below unknown
    allowMasterDownConnections("allowMasterDownConnections", "false", Boolean.class), //
    allowSlaveDownConnections("allowSlaveDownConnections", "false", Boolean.class), //
    loadBalanceBlacklistTimeout("loadBalanceBlacklistTimeout", "0", Integer.class), //
    queriesBeforeRetryMaster("queriesBeforeRetryMaster", "50", Integer.class), //
    readFromMasterWhenNoSlaves("readFromMasterWhenNoSlaves", "false", Boolean.class), //
    secondsBeforeRetryMaster("secondsBeforeRetryMaster", "30", Object.class), //
    xdevapiAsyncResponseTimeout("xdevapi.asyncResponseTimeout", "xdevapiAsyncResponseTimeout", null, Object.class), //
    xdevapiUseAsyncProtocol("xdevapi.useAsyncProtocol", "xdevapiUseAsyncProtocol", null, Object.class), //

    @OnlyReactor
    timeTruncateFractional("timeTruncateFractional", "true", Boolean.class),
    @OnlyReactor
    clientPrepareSupportStream("false", Boolean.class),
    factoryWorkerCount("50", Integer.class);

    private final String keyName;
    private final String ccAlias;
    private final String defaultValue;
    private final Class<?> javaType;
    private final boolean caseSensitive;


    MyKey(Class<?> javaType) {
        this(null, null, null, javaType, true);
    }

    MyKey(Class<?> javaType, boolean caseSensitive) {
        this(null, null, null, javaType, caseSensitive);
    }


    MyKey(@Nullable String defaultValue, Class<?> javaType) {
        this(null, null, defaultValue, javaType, true);
    }

    MyKey(@Nullable String defaultValue, Class<?> javaType, boolean caseSensitive) {
        this(null, null, defaultValue, javaType, caseSensitive);
    }

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     *
     * @param keyName the key name for the enum element.
     */
    MyKey(String keyName, @Nullable String defaultValue, Class<?> javaType) {
        this(keyName, null, defaultValue, javaType, true);
    }

    MyKey(String keyName, @Nullable String alias, @Nullable String defaultValue, Class<?> javaType) {
        this(keyName, alias, defaultValue, javaType, true);
    }

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     *
     * @param keyName       the key name for the enum element.
     * @param alias         camel-case alias key name
     * @param caseSensitive is this name case sensitive
     */
    MyKey(@Nullable String keyName, @Nullable String alias, @Nullable String defaultValue, Class<?> javaType
            , boolean caseSensitive) {
        this.keyName = keyName == null ? name() : keyName;
        this.ccAlias = alias;
        this.defaultValue = defaultValue;
        this.javaType = javaType;

        this.caseSensitive = caseSensitive;
    }


    @Override
    public String toString() {
        return this.keyName;
    }

    /**
     * Gets the key name of this enum element.
     *
     * @return the key name associated with the enum element.
     */
    @Override
    public String getKey() {
        return this.keyName;
    }

    @Nullable
    @Override
    public String getAlias() {
        return this.ccAlias;
    }


    @Nullable
    public String getDefault() {
        return this.defaultValue;
    }

    public String getRequiredDefault() {
        return Objects.requireNonNull(this.defaultValue, "defaultValue");
    }


    @Override
    public Class<?> getJavaType() {
        return this.javaType;
    }

    @Override
    public boolean isCaseSensitive() {
        return this.caseSensitive;
    }


}
