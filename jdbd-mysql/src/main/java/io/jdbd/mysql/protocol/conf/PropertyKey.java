package io.jdbd.mysql.protocol.conf;

import io.jdbd.mysql.protocol.ClientConstants;
import io.jdbd.mysql.protocol.Constants;
import io.jdbd.mysql.protocol.authentication.MySQLNativePasswordPlugin;
import io.jdbd.mysql.protocol.client.Enums;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.conf.IPropertyKey;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.annotation.Nullable;

import java.util.Objects;

/**
 * PropertyKey handles connection property names, their camel-case aliases and case sensitivity.
 *
 * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-configuration-properties.html">Configuration Properties</a>
 */
public enum PropertyKey implements IPropertyKey {
    /*
     * Properties individually managed after parsing connection string. These property keys are case insensitive.
     */
    //below Authentication Group
    /** The database user name. */
    USER(HostInfo.USER, null, false),
    /** The database user password. */
    PASSWORD(HostInfo.PASSWORD, null, false),


    /** The hostname value from the properties instance passed to the driver. */
    HOST(HostInfo.HOST, null, false),
    /** The port number value from the properties instance passed to the driver. */
    PORT(HostInfo.PORT, null, false),

    /** The communications protocol. Possible values: "tcp" and "pipe". */
    PROTOCOL("protocol", null, false),
    /** The name pipes path to use when "protocol=pipe'. */
    PATH("path", "namedPipePath", false),
    /** The server type in a replication setup. Possible values: "master" and "slave". */
    TYPE("type", null, false),
    /** The address value ("host:port") from the properties instance passed to the driver. */
    ADDRESS("address", null, false),

    /** The host priority in a list of hosts. */
    PRIORITY("priority", null, false),
    /** The database value from the properties instance passed to the driver. */
    DBNAME("dbname", null, false), //

    // blow Connection Group
    connectionAttributes("connectionAttributes", null, true), //
    connectionLifecycleInterceptors("connectionLifecycleInterceptors", null, true), //
    useConfigs("useConfigs", null, true), //
    authenticationPlugins("authenticationPlugins", null, true), //

    /** @deprecated discard in jdbd */
    @Deprecated
    clientInfoProvider("clientInfoProvider", "com.mysql.cj.jdbc.CommentClientInfoProvider", true), //
    createDatabaseIfNotExist("createDatabaseIfNotExist", "false", true), //
    /** @deprecated always CATALOG in jdbd */
    @Deprecated
    databaseTerm("databaseTerm", "CATALOG", true), //
    defaultAuthenticationPlugin("defaultAuthenticationPlugin", MySQLNativePasswordPlugin.PLUGIN_NAME, true), //

    detectCustomCollations("detectCustomCollations", "false", true), //
    disabledAuthenticationPlugins("disabledAuthenticationPlugins", null, true), //
    disconnectOnExpiredPasswords("disconnectOnExpiredPasswords", "true", true), //
    interactiveClient("interactiveClient", "false", true), //
    passwordCharacterEncoding("passwordCharacterEncoding", null, true), //
    propertiesTransform("propertiesTransform", null, true), //
    rollbackOnPooledClose("rollbackOnPooledClose", "true", true), //
    useAffectedRows("useAffectedRows", "false", true), //


    // blow Session Group
    sessionVariables("sessionVariables", null, true), //
    /**
     * relation to System Variables {@code character_set_client}
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html">character_set_client</a>
     */
    characterEncoding("characterEncoding", null, true), //
    /**
     * relation to System Variables {@code character_set_results }
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html">character_set_results</a>
     */
    characterSetResults("characterSetResults", null, true), //
    /**
     * relation to System Variables {@code collation_connection  }
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html">collation_connection </a>
     */
    connectionCollation("connectionCollation", null, true), //

    // blow Networking Group
    socksProxyHost("socksProxyHost", null, true), //
    socksProxyPort("socksProxyPort", "1080", true), //
    /** @deprecated use {@link #connectionProvider} */
    @Deprecated
    socketFactory("socketFactory", null, true), //
    /** must be class name of implementation of {@link ConnectionProvider } */
    connectionProvider("connectionProvider", null, true),
    connectTimeout("connectTimeout", "0", true), //
    socketTimeout("socketTimeout", "0", true), //
    dnsSrv("dnsSrv", "false", true), //
    localSocketAddress("localSocketAddress", null, true), //
    maxAllowedPacket("maxAllowedPacket", "65535", true), //
    tcpKeepAlive("tcpKeepAlive", "true", true), //
    tcpNoDelay("tcpNoDelay", "true", true), //
    tcpRcvBuf("tcpRcvBuf", "0", true), //
    tcpSndBuf("tcpSndBuf", "0", true), //


    // blow Security Group
    /**
     * @see Enums.SslMode
     */
    sslMode("sslMode", "PREFERRED", true), //
    /** @deprecated use {@link #sslMode} */
    @Deprecated
    useSSL("useSSL", "true", true), //
    /** @deprecated use {@link #sslMode} */
    @Deprecated
    requireSSL("requireSSL", "false", true), //
    /** @deprecated use {@link #sslMode} */
    @Deprecated
    verifyServerCertificate("verifyServerCertificate", "false", true), //

    fallbackToSystemKeyStore("fallbackToSystemKeyStore", "true", true), //
    fallbackToSystemTrustStore("fallbackToSystemTrustStore", "true", true), //
    bigRowMemoryUpperBoundary("bigRowMemoryUpperBoundary", Integer.toString(ClientConstants.MIN_BIG_ROW_UPPER), true),

    allowLoadLocalInfile("allowLoadLocalInfile", "false", true), //
    allowMasterDownConnections("allowMasterDownConnections", "false", true), //

    allowMultiQueries("allowMultiQueries", "false", true), //
    allowNanAndInf("allowNanAndInf", "", true), //
    allowPublicKeyRetrieval("allowPublicKeyRetrieval", "false", true), //
    allowSlaveDownConnections("allowSlaveDownConnections", "false", true), //

    allowUrlInLocalInfile("allowUrlInLocalInfile", "false", true), //
    alwaysSendSetIsolation("alwaysSendSetIsolation", "true", true), //

    autoClosePStmtStreams("autoClosePStmtStreams", "false", true), //

    autoDeserialize("autoDeserialize", "false", true), //
    autoGenerateTestcaseScript("autoGenerateTestcaseScript", "false", true), //
    autoReconnect("autoReconnect", "false", true), //
    autoReconnectForPools("autoReconnectForPools", "false", true), //

    autoSlowLog("autoSlowLog", "true", true), //
    blobsAreStrings("blobsAreStrings", "false", true), //
    blobSendChunkSize("blobSendChunkSize", "1048576", true), //
    cacheCallableStmts("cacheCallableStmts", "false", true), //

    cacheDefaultTimezone("cacheDefaultTimezone", "true", true), //
    cachePrepStmts("cachePrepStmts", "false", true), //
    cacheResultSetMetadata("cacheResultSetMetadata", "false", true), //
    cacheServerConfiguration("cacheServerConfiguration", "false", true), //

    callableStmtCacheSize("callableStmtCacheSize", "100", true), //


    clientCertificateKeyStorePassword("clientCertificateKeyStorePassword", null, true), //

    clientCertificateKeyStoreType("clientCertificateKeyStoreType", "JKS", true), //
    clientCertificateKeyStoreUrl("clientCertificateKeyStoreUrl", null, true), //

    clobberStreamingResults("clobberStreamingResults", "false", true), //

    clobCharacterEncoding("clobCharacterEncoding", null, true), //
    compensateOnDuplicateKeyUpdateCounts("compensateOnDuplicateKeyUpdateCounts", "false", true), //


    continueBatchOnError("continueBatchOnError", "true", true), //


    defaultFetchSize("defaultFetchSize", "0", true), //


    dontCheckOnDuplicateKeyUpdateInSQL("dontCheckOnDuplicateKeyUpdateInSQL", "false", true), //

    dontTrackOpenResources("dontTrackOpenResources", "false", true), //
    dumpQueriesOnException("dumpQueriesOnException", "false", true), //
    elideSetAutoCommits("elideSetAutoCommits", "false", true), //
    emptyStringsConvertToZero("emptyStringsConvertToZero", "true", true), //

    emulateLocators("emulateLocators", "false", true), //
    emulateUnsupportedPstmts("emulateUnsupportedPstmts", "true", true), //
    enabledSSLCipherSuites("enabledSSLCipherSuites", null, true), //
    enabledTLSProtocols("enabledTLSProtocols", null, true), //
    enableEscapeProcessing("enableEscapeProcessing", "true", true), //

    enablePacketDebug("enablePacketDebug", "false", true), //
    enableQueryTimeouts("enableQueryTimeouts", "true", true), //
    exceptionInterceptors("exceptionInterceptors", null, true), //
    explainSlowQueries("explainSlowQueries", "false", true), //

    failOverReadOnly("failOverReadOnly", "true", true), //
    functionsNeverReturnBlobs("functionsNeverReturnBlobs", "false", true), //
    gatherPerfMetrics("gatherPerfMetrics", "false", true), //
    generateSimpleParameterMetadata("generateSimpleParameterMetadata", "false", true), //

    getProceduresReturnsFunctions("getProceduresReturnsFunctions", "true", true), //
    holdResultsOpenOverStatementClose("holdResultsOpenOverStatementClose", "false", true), //
    ha_enableJMX("ha.enableJMX", "haEnableJMX", "false", true), //
    ha_loadBalanceStrategy("ha.loadBalanceStrategy", "haLoadBalanceStrategy", "random", true), //

    ignoreNonTxTables("ignoreNonTxTables", "false", true), //
    includeInnodbStatusInDeadlockExceptions("includeInnodbStatusInDeadlockExceptions", "false", true), //
    includeThreadDumpInDeadlockExceptions("includeThreadDumpInDeadlockExceptions", "false", true), //
    includeThreadNamesAsStatementComment("includeThreadNamesAsStatementComment", "false", true), //

    initialTimeout("initialTimeout", "2", true), //

    jdbcCompliantTruncation("jdbcCompliantTruncation", "true", true), //
    largeRowSizeThreshold("largeRowSizeThreshold", "2048", true), //

    loadBalanceAutoCommitStatementRegex("loadBalanceAutoCommitStatementRegex", null, true), //
    loadBalanceAutoCommitStatementThreshold("loadBalanceAutoCommitStatementThreshold", "0", true), //
    loadBalanceBlacklistTimeout("loadBalanceBlacklistTimeout", "0", true), //
    loadBalanceConnectionGroup("loadBalanceConnectionGroup", null, true), //

    loadBalanceExceptionChecker("loadBalanceExceptionChecker", "com.mysql.cj.jdbc.ha.StandardLoadBalanceExceptionChecker", true), //
    loadBalanceHostRemovalGracePeriod("loadBalanceHostRemovalGracePeriod", "15000", true), //
    loadBalancePingTimeout("loadBalancePingTimeout", "0", true), //
    loadBalanceSQLStateFailover("loadBalanceSQLStateFailover", null, true), //

    loadBalanceSQLExceptionSubclassFailover("loadBalanceSQLExceptionSubclassFailover", null, true), //
    loadBalanceValidateConnectionOnSwapServer("loadBalanceValidateConnectionOnSwapServer", "false", true), //

    locatorFetchBufferSize("locatorFetchBufferSize", "1048576", true), //

    logger("logger", "com.mysql.cj.log.StandardLogger", true), //
    logSlowQueries("logSlowQueries", "false", true), //
    logXaCommands("logXaCommands", "false", true), //
    maintainTimeStats("maintainTimeStats", "true", true), //


    maxQuerySizeToLog("maxQuerySizeToLog", "2048", true), //
    maxReconnects("maxReconnects", "3", true), //
    maxRows("maxRows", "-1", true), //

    metadataCacheSize("metadataCacheSize", "50", true), //
    netTimeoutForStreamingResults("netTimeoutForStreamingResults", "600", true), //
    noAccessToProcedureBodies("noAccessToProcedureBodies", "false", true), //
    noDatetimeStringSync("noDatetimeStringSync", "false", true), //

    nullDatabaseMeansCurrent("nullDatabaseMeansCurrent", "nullCatalogMeansCurrent", "false", true), //
    overrideSupportsIntegrityEnhancementFacility("overrideSupportsIntegrityEnhancementFacility", "false", true), //
    packetDebugBufferSize("packetDebugBufferSize", "20", true), //
    padCharsWithSpace("padCharsWithSpace", "false", true), //

    paranoid("paranoid", "false", false), //
    parseInfoCacheFactory("parseInfoCacheFactory", "com.mysql.cj.PerConnectionLRUFactory", true), //

    pedantic("pedantic", "false", true), //

    pinGlobalTxToPhysicalConnection("pinGlobalTxToPhysicalConnection", "false", true), //
    populateInsertRowWithDefaultValues("populateInsertRowWithDefaultValues", "false", true), //
    prepStmtCacheSize("prepStmtCacheSize", "25", true), //
    prepStmtCacheSqlLimit("prepStmtCacheSqlLimit", "256", true), //

    processEscapeCodesForPrepStmts("processEscapeCodesForPrepStmts", "true", true), //
    profilerEventHandler("profilerEventHandler", "com.mysql.cj.log.LoggingProfilerEventHandler", true), //
    profileSQL("profileSQL", "false", true), //


    queriesBeforeRetryMaster("queriesBeforeRetryMaster", "50", true), //
    queryInterceptors("queryInterceptors", null, true), //
    queryTimeoutKillsConnection("queryTimeoutKillsConnection", "false", true), //
    readFromMasterWhenNoSlaves("readFromMasterWhenNoSlaves", "false", true), //

    readOnlyPropagatesToServer("readOnlyPropagatesToServer", "true", true), //
    reconnectAtTxEnd("reconnectAtTxEnd", "false", true), //
    replicationConnectionGroup("replicationConnectionGroup", null, true), //
    reportMetricsIntervalMillis("reportMetricsIntervalMillis", "30000", true), //


    resourceId("resourceId", null, true), //
    resultSetSizeThreshold("resultSetSizeThreshold", "100", true), //
    retriesAllDown("retriesAllDown", "120", true), //

    rewriteBatchedStatements("rewriteBatchedStatements", "false", true), //

    secondsBeforeRetryMaster("secondsBeforeRetryMaster", "30", true), //
    selfDestructOnPingMaxOperations("selfDestructOnPingMaxOperations", "0", true), //

    selfDestructOnPingSecondsLifetime("selfDestructOnPingSecondsLifetime", "0", true), //
    sendFractionalSeconds("sendFractionalSeconds", "true", true), //
    serverAffinityOrder("serverAffinityOrder", null, true), //
    serverConfigCacheFactory("serverConfigCacheFactory", "com.mysql.cj.util.PerVmServerConfigCacheFactory", true), //
    serverRSAPublicKeyFile("serverRSAPublicKeyFile", null, true), //

    slowQueryThresholdMillis("slowQueryThresholdMillis", "2000", true), //
    slowQueryThresholdNanos("slowQueryThresholdNanos", "0", true), //


    strictUpdates("strictUpdates", "true", true), //

    connectionTimeZone(null, Constants.LOCAL, true),

    tcpTrafficClass("tcpTrafficClass", "0", true), //
    tinyInt1isBit("tinyInt1isBit", "true", true), //
    traceProtocol("traceProtocol", "false", true), //

    transformedBitIsBoolean("transformedBitIsBoolean", "false", true), //
    treatUtilDateAsTimestamp("treatUtilDateAsTimestamp", "true", true), //
    trustCertificateKeyStorePassword("trustCertificateKeyStorePassword", null, true), //
    trustCertificateKeyStoreType("trustCertificateKeyStoreType", "JKS", true), //

    trustCertificateKeyStoreUrl("trustCertificateKeyStoreUrl", null, true), //
    ultraDevHack("ultraDevHack", "false", true), //

    useColumnNamesInFindColumn("useColumnNamesInFindColumn", "false", true), //

    useCompression("useCompression", "false", true), //

    useCursorFetch("useCursorFetch", "false", true), //
    useHostsInPrivileges("useHostsInPrivileges", "true", true), //

    useInformationSchema("useInformationSchema", "false", true), //
    useLocalSessionState("useLocalSessionState", "false", true), //
    useLocalTransactionState("useLocalTransactionState", "false", true), //
    useNanosForElapsedTime("useNanosForElapsedTime", "false", true), //

    useOldAliasMetadataBehavior("useOldAliasMetadataBehavior", "false", true), //
    useOnlyServerErrorMessages("useOnlyServerErrorMessages", "true", true), //
    useReadAheadInput("useReadAheadInput", "true", true), //
    useServerPrepStmts("useServerPrepStmts", "false", true), //


    useStreamLengthsInPrepStmts("useStreamLengthsInPrepStmts", "true", true), //
    useUnbufferedInput("useUnbufferedInput", "true", true), //
    useUsageAdvisor("useUsageAdvisor", "false", true), //


    xdevapiAsyncResponseTimeout("xdevapi.asyncResponseTimeout", "xdevapiAsyncResponseTimeout", null, true), //
    xdevapiAuth("xdevapi.auth", "xdevapiAuth", "PLAIN", true), //
    xdevapiConnectTimeout("xdevapi.connect-timeout", "xdevapiConnectTimeout", "10000", true), //

    xdevapiConnectionAttributes("xdevapi.connection-attributes", "xdevapiConnectionAttributes", true), //
    xdevapiCompression("xdevapi.compression", "xdevapiCompression", "PREFERRED", true), //
    xdevapiCompressionAlgorithm("xdevapi.compression-algorithm", "xdevapiCompressionAlgorithm", null, true), //
    xdevapiDnsSrv("xdevapi.dns-srv", "xdevapiDnsSrv", "false", true), //

    xdevapiSSLMode("xdevapi.ssl-mode", "xdevapiSSLMode", "REQUIRED", true), //
    xdevapiTlsCiphersuites("xdevapi.tls-ciphersuites", "xdevapiTlsCiphersuites", null, true), //
    xdevapiTlsVersions("xdevapi.tls-versions", "xdevapiTlsVersions", null, true), //
    xdevapiSSLTrustStoreUrl("xdevapi.ssl-truststore", "xdevapiSSLTruststore", null, true), //

    xdevapiSSLTrustStoreType("xdevapi.ssl-truststore-type", "xdevapiSSLTruststoreType", "JKS", true), //
    xdevapiSSLTrustStorePassword("xdevapi.ssl-truststore-password", "xdevapiSSLTruststorePassword", null, true), //
    xdevapiUseAsyncProtocol("xdevapi.useAsyncProtocol", "xdevapiUseAsyncProtocol", null, true), //
    yearIsDateType("yearIsDateType", "true", true), //

    zeroDateTimeBehavior("zeroDateTimeBehavior", "EXCEPTION", true), //

    // new add
    clientPrepare("clientPrepare", "UN_SUPPORT_STREAM", true);

    private final String keyName;
    private final String ccAlias;
    private final String defaultValue;
    private final boolean isCaseSensitive;

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     *
     * @param keyName         the key name for the enum element.
     * @param isCaseSensitive is this name case sensitive
     */
    PropertyKey(String keyName, @Nullable String defaultValue, boolean isCaseSensitive) {
        this(keyName, null, defaultValue, isCaseSensitive);
    }

    /**
     * Initializes each enum element with the proper key name to be used in the connection string or properties maps.
     *
     * @param keyName         the key name for the enum element.
     * @param alias           camel-case alias key name
     * @param isCaseSensitive is this name case sensitive
     */
    PropertyKey(String keyName, @Nullable String alias, @Nullable String defaultValue, boolean isCaseSensitive) {
        this.keyName = keyName;
        this.ccAlias = alias;
        this.defaultValue = defaultValue;
        this.isCaseSensitive = isCaseSensitive;
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

    /**
     * Gets the camel-case alias key name of this enum element.
     *
     * @return the camel-case alias key name associated with the enum element or null.
     */
    @Nullable
    public String getCcAlias() {
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
    public Class<?> getType() {
        return null;
    }
}
