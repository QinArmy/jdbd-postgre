package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.MySQLJdbdException;
import io.jdbd.mysql.env.MySQLHostEnv;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.protocol.X509TrustManagerWrapper;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.util.MySQLStates;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.env.HostInfo;
import io.jdbd.vendor.env.Properties;
import io.jdbd.vendor.util.SQLStates;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.*;
import io.qinarmy.util.Pair;
import reactor.util.annotation.Nullable;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.*;
import java.security.cert.CertificateException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @see SslHandler
 */
final class ReactorSslProviderBuilder {


    static ReactorSslProviderBuilder builder() {
        return new ReactorSslProviderBuilder();
    }

    private MySQLHostEnv host;

    private MySQLServerVersion serverVersion;

    private ByteBufAllocator allocator;

    private Properties properties;


    private ReactorSslProviderBuilder() {
    }

    public ReactorSslProviderBuilder hostInfo(MySQLHostEnv host) {
        this.host = host;
        return this;
    }

    public ReactorSslProviderBuilder serverVersion(MySQLServerVersion serverVersion) {
        this.serverVersion = serverVersion;
        return this;
    }

    public ReactorSslProviderBuilder allocator(ByteBufAllocator allocator) {
        this.allocator = allocator;
        return this;
    }


    public reactor.netty.tcp.SslProvider build() throws SQLException {

        return reactor.netty.tcp.SslProvider.builder()
                .sslContext(buildSslContext())
                .build();
    }

    public SslHandler buildSslHandler() throws JdbdException {
        HostInfo hostInfo = this.hostInfo;
        return buildSslContext().newHandler(this.allocator, hostInfo.getHost(), hostInfo.getPort());
    }

    public SslContext buildSslContext() throws JdbdException {
        if (this.hostInfo == null || this.serverVersion == null || this.allocator == null) {
            throw new IllegalStateException("hostInfo or serverVersion or allocator is null");
        }
        this.properties = this.hostInfo.getProperties();

        SslContextBuilder builder = SslContextBuilder.forClient();
        // 1. config TrustManager
        configTrustManager(builder);
        //2. config KeyManager
        KeyManagerFactory keyManagerFactory = tryObtainKeyManagerFactory();
        if (keyManagerFactory != null) {
            builder.keyManager(keyManagerFactory);
        }
        //3. config ssl protocol.
        builder.protocols(obtainAllowedTlsProtocolList())
                // 4. config cipher suit.
                .ciphers(obtainAllowedCipherSuitList(), SupportedCipherSuiteFilter.INSTANCE);
        // 5. config provider
        if (OpenSsl.isAvailable()) {
            builder.sslProvider(SslProvider.OPENSSL);
        } else {
            builder.sslProvider(SslProvider.JDK);
        }
        try {
            return builder.build();
        } catch (SSLException e) {
            String message = String.format("Cannot create %s due to [%s]", SslHandler.class.getName(), e.getMessage());
            throw new SQLException(message, MySQLStates.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, e);
        }
    }


    private List<String> obtainAllowedCipherSuitList() {
        String enabledSSLCipherSuites = this.properties.get(MyKey.enabledSSLCipherSuites);
        List<String> candidateList = MySQLStrings.spitAsList(enabledSSLCipherSuites, ",");

        if (candidateList.isEmpty()) {
            candidateList = SslUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST;
        } else {
            candidateList.retainAll(SslUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST);
        }

        List<String> allowedCipherList = new ArrayList<>(candidateList.size());
        candidateFor:
        for (String candidate : candidateList) {
            for (String restricted : SslUtils.MYSQL_RESTRICTED_CIPHER_SUBSTR_LIST) {
                if (candidate.contains(restricted)) {
                    continue candidateFor;
                }
            }
            allowedCipherList.add(candidate);
        }
        return Collections.unmodifiableList(allowedCipherList);
    }

    /**
     * @return a unmodifiable list
     */
    private List<String> obtainAllowedTlsProtocolList() {
        String enabledTLSProtocols = this.properties.get(MyKey.enabledTLSProtocols);
        List<String> candidateList = MySQLStrings.spitAsList(enabledTLSProtocols, ",");

        if (candidateList.isEmpty()) {
            MySQLServerVersion serverVersion = this.serverVersion;

            if (serverVersion.meetsMinimum(5, 7, 28)
                    || (serverVersion.meetsMinimum(5, 6, 46) && !serverVersion.meetsMinimum(5, 7, 0))
                    || (serverVersion.meetsMinimum(5, 6, 0) && MySQLServerVersion.isEnterpriseEdition(serverVersion))) {
                candidateList = SslUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST;
            } else {
                candidateList = Collections.unmodifiableList(Arrays.asList(SslUtils.TLSv1_1, SslUtils.TLSv1));
            }
        } else {
            List<String> supportProtocolList = new ArrayList<>(SslUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST);
            supportProtocolList.retainAll(candidateList);
            candidateList = Collections.unmodifiableList(supportProtocolList);
        }
        return candidateList;
    }


    private void configTrustManager(final io.netty.handler.ssl.SslContextBuilder builder) throws SQLException {
        Enums.SslMode sslMode;
        sslMode = this.properties.get(MyKey.sslMode, Enums.SslMode.class);
        final boolean verify = sslMode == Enums.SslMode.VERIFY_CA
                || sslMode == Enums.SslMode.VERIFY_IDENTITY;
        try {

            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(false);

            if (storePair != null || verify) {
                TrustManagerFactory tmfWrapper = new TrustManagerFactoryWrapper(
                        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
                        , verify, this.hostInfo.getHost());

                if (storePair == null) {
                    tmfWrapper.init((KeyStore) null); //initializes the TrustManagerFactory with the default truststore.
                } else {
                    tmfWrapper.init(storePair.getFirst());
                }
                builder.trustManager(tmfWrapper);
            } else {
                builder.trustManager(new X509TrustManagerWrapper(this.hostInfo.getHost()));

            }

        } catch (NoSuchAlgorithmException e) {
            String message = String.format("%s algorithm[%s] not found.", TrustManagerFactory.class.getName()
                    , TrustManagerFactory.getDefaultAlgorithm());
            throw new SQLException(message, MySQLStates.CONNECTION_EXCEPTION, e);
        } catch (KeyStoreException e) {
            String message = String.format("Cannot init %s due to %s", TrustManagerFactory.class.getName()
                    , e.getMessage());
            throw new MySQLJdbdException(message, MySQLStates.CONNECTION_EXCEPTION, e);
        }
    }

    @Nullable
    private KeyManagerFactory tryObtainKeyManagerFactory() throws SQLException {
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(true);
            if (storePair == null) {
                return null;
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(storePair.getFirst(), storePair.getSecond());
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            String message = String.format("%s algorithm[%s] not found.", KeyManagerFactory.class.getName()
                    , KeyManagerFactory.getDefaultAlgorithm());
            throw new SQLException(message, MySQLStates.CONNECTION_EXCEPTION, e);
        } catch (KeyStoreException | UnrecoverableKeyException e) {
            String message = String.format("Cannot init %s due to %s", KeyManagerFactory.class.getName(), e.getMessage());
            throw new MySQLJdbdException(message, MySQLStates.CONNECTION_EXCEPTION, e);
        }
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">Security</a>
     */
    @Nullable
    private Pair<KeyStore, char[]> tryObtainKeyStorePasswordPairForSsl(final boolean key) throws SQLException {
        // 1. below obtain three storeUrl,storeType,storePassword
        final MyKey storeUrlKey, storeTypeKey, passwordKey;
        final String systemStoreUrlKey, systemStoreTypeKey, systemPasswordKey;
        if (key) {
            storeUrlKey = MyKey.clientCertificateKeyStoreUrl;
            storeTypeKey = MyKey.clientCertificateKeyStoreType;
            passwordKey = MyKey.clientCertificateKeyStorePassword;

            systemStoreUrlKey = "javax.net.ssl.keyStore";
            systemStoreTypeKey = "javax.net.ssl.keyStoreType";
            systemPasswordKey = "javax.net.ssl.keyStorePassword";
        } else {
            storeUrlKey = MyKey.trustCertificateKeyStoreUrl;
            storeTypeKey = MyKey.trustCertificateKeyStoreType;
            passwordKey = MyKey.trustCertificateKeyStorePassword;

            systemStoreUrlKey = "javax.net.ssl.trustStore";
            systemStoreTypeKey = "javax.net.ssl.trustStoreType";
            systemPasswordKey = "javax.net.ssl.trustStorePassword";
        }

        final Properties properties = this.properties;
        String storeUrl, storeType, storePwd;

        storeUrl = properties.get(storeUrlKey);
        storeType = properties.get(storeTypeKey);
        storePwd = properties.get(passwordKey);

        if (!MySQLStrings.hasText(storeUrl)) {
            boolean useSystem = (key && properties.getOrDefault(MyKey.fallbackToSystemKeyStore, Boolean.class))
                    || (!key && properties.getOrDefault(MyKey.fallbackToSystemTrustStore, Boolean.class));
            if (useSystem) {
                storeUrl = System.getProperty(systemStoreUrlKey);
                storeType = System.getProperty(systemStoreTypeKey);
                storePwd = System.getProperty(systemPasswordKey);
            }
            if (!MySQLStrings.hasText(storeType)) {
                storeType = properties.get(storeTypeKey);
            }

        }

        if (MySQLStrings.hasText(storeUrl)) {
            try {
                new URL(storeUrl);
            } catch (MalformedURLException e) {
                storeUrl = "file:" + storeUrl;
            }
            if (!MySQLStrings.hasText(storeType)) {
                return null;
            }
        } else {
            return null;
        }

        final char[] storePassword = (storePwd == null) ? new char[0] : storePwd.toCharArray();
        // 2. create and init KeyStore with three storeUrl,storeType,storePassword
        try (InputStream storeInput = new URL(storeUrl).openStream()) {
            KeyStore keyStore = KeyStore.getInstance(storeType);
            keyStore.load(storeInput, storePassword);
            return new Pair<>(keyStore, storePassword);
        } catch (MalformedURLException e) {
            throw new SQLException(String.format("%s[%s] isn't url.", storeUrlKey, storeUrl)
                    , MySQLStates.CONNECTION_EXCEPTION, e);
        } catch (KeyStoreException e) {
            throw new SQLException(String.format("%s[%s] is KeyStore type than is supported by provider."
                    , storeTypeKey, storeType)
                    , MySQLStates.CONNECTION_EXCEPTION, e);
        } catch (NoSuchAlgorithmException | IOException | CertificateException e) {
            String message = String.format("Cannot load KeyStore by url[%s] and type[%s]", storeUrl, storeType);
            throw new SQLException(message, MySQLStates.CONNECTION_EXCEPTION, e);
        }
    }


    private static final class TrustManagerFactoryWrapper extends TrustManagerFactory {

        private TrustManagerFactoryWrapper(TrustManagerFactory factory, boolean verify, String host) {
            super(new TrustManagerFactorySpiWrapper(factory, verify, host)
                    , factory.getProvider(), factory.getAlgorithm());
        }


    }

    private static final class TrustManagerFactorySpiWrapper extends TrustManagerFactorySpi {

        private final TrustManagerFactory factory;

        private final boolean verify;

        private final String host;

        private TrustManagerFactorySpiWrapper(TrustManagerFactory factory, boolean verify, String host) {
            this.factory = factory;
            this.verify = verify;
            this.host = host;
        }

        @Override
        protected void engineInit(KeyStore keyStore) throws KeyStoreException {
            this.factory.init(keyStore);
        }

        @Override
        protected void engineInit(ManagerFactoryParameters managerFactoryParameters)
                throws InvalidAlgorithmParameterException {
            this.factory.init(managerFactoryParameters);
        }

        @Override
        protected TrustManager[] engineGetTrustManagers() {
            TrustManager[] trustManagerArray = this.factory.getTrustManagers();
            TrustManager[] wrapperArray = new TrustManager[trustManagerArray.length];

            try {
                for (int i = 0; i < trustManagerArray.length; i++) {
                    TrustManager tm = trustManagerArray[i];

                    if (tm instanceof X509TrustManager) {
                        wrapperArray[i] = new X509TrustManagerWrapper((X509TrustManager) tm, this.verify
                                , this.host);
                    } else {
                        wrapperArray[i] = tm;
                    }

                }
            } catch (Throwable e) {
                String message = String.format("Can't create %s due to %s."
                        , X509TrustManagerWrapper.class.getName(), e.getMessage());
                throw new JdbdSQLException(new SQLException(message, SQLStates.CONNECTION_EXCEPTION));
            }
            return wrapperArray;
        }

    }


}
