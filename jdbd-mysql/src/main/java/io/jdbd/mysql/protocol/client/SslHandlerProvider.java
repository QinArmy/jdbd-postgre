package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.ServerVersion;
import io.jdbd.mysql.protocol.conf.HostInfo;
import io.jdbd.mysql.protocol.conf.Properties;
import io.jdbd.mysql.protocol.conf.PropertyDefinitions;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.qinarmy.util.Pair;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

final class SslHandlerProvider {


    static SslHandlerProvider create(HostInfo hostInfo, ServerVersion serverVersion, ByteBufAllocator allocator) {
        return new SslHandlerProvider(hostInfo, serverVersion, allocator);
    }

    private final HostInfo hostInfo;

    private final ServerVersion serverVersion;

    private final ByteBufAllocator allocator;

    private final Properties properties;


    private SslHandlerProvider(HostInfo hostInfo, ServerVersion serverVersion, ByteBufAllocator allocator) {
        this.hostInfo = hostInfo;
        this.serverVersion = serverVersion;
        this.allocator = allocator;
        this.properties = hostInfo.getProperties();
    }

    SslHandler acquire() {
        TrustManagerFactory trustManagerFactory = tryObtainTrustManagerFactory();
        KeyManagerFactory keyManagerFactory = tryObtainKeyManagerFactory();

        SslContextBuilder contextBuilder = SslContextBuilder.forClient();
        if (trustManagerFactory != null) {
            contextBuilder.trustManager(trustManagerFactory);
        }
        if (keyManagerFactory != null) {
            contextBuilder.keyManager(keyManagerFactory);
        }
        contextBuilder.protocols(obtainAllowedTlsProtocolList())
        //TODO zoro 找到确认 jdk or open ssl 支持 cipher suit 的方法
        //.ciphers(obtainAllowedCipherSuitList())
        ;
        try {
            HostInfo hostInfo = this.hostInfo;
            return contextBuilder
                    .build()
                    .newHandler(this.allocator, hostInfo.getHost(), hostInfo.getPort());
        } catch (SSLException e) {
            throw new JdbdMySQLException(e, "create %s", SslContext.class.getName());
        }
    }


    private List<String> obtainAllowedCipherSuitList() {
        String enabledSSLCipherSuites = this.properties.getProperty(PropertyKey.enabledSSLCipherSuites);
        List<String> candidateList = MySQLStringUtils.spitAsList(enabledSSLCipherSuites, ",");

        if (candidateList.isEmpty()) {
            candidateList = ProtocolUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST;
        } else {
            candidateList.retainAll(ProtocolUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST);
        }

        List<String> allowedCipherList = new ArrayList<>(candidateList.size());
        candidateFor:
        for (String candidate : candidateList) {
            for (String restricted : ProtocolUtils.MYSQL_RESTRICTED_CIPHER_SUBSTR_LIST) {
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
        String enabledTLSProtocols = this.properties.getProperty(PropertyKey.enabledTLSProtocols);
        List<String> candidateList = MySQLStringUtils.spitAsList(enabledTLSProtocols, ",");

        if (candidateList.isEmpty()) {
            ServerVersion serverVersion = this.serverVersion;

            if (serverVersion.meetsMinimum(5, 7, 28)
                    || (serverVersion.meetsMinimum(5, 6, 46) && !serverVersion.meetsMinimum(5, 7, 0))
                    || (serverVersion.meetsMinimum(5, 6, 0) && ServerVersion.isEnterpriseEdition(serverVersion))) {
                candidateList = ProtocolUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST;
            } else {
                candidateList = Collections.unmodifiableList(Arrays.asList(ProtocolUtils.TLSv1_1, ProtocolUtils.TLSv1));
            }
        } else {
            candidateList.retainAll(ProtocolUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST);
            candidateList = Collections.unmodifiableList(candidateList);
        }
        return candidateList;
    }


    @Nullable
    private TrustManagerFactory tryObtainTrustManagerFactory() {
        PropertyDefinitions.SslMode sslMode;
        sslMode = this.properties.getProperty(PropertyKey.sslMode, PropertyDefinitions.SslMode.class);
        if (sslMode != PropertyDefinitions.SslMode.VERIFY_CA
                && sslMode != PropertyDefinitions.SslMode.VERIFY_IDENTITY) {
            return null;
        }
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(false);
            if (storePair == null) {
                return null;
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(storePair.getFirst());
            return tmf;
        } catch (NoSuchAlgorithmException e) {
            // use default algorithm, never here.
            throw new JdbdMySQLException(e, "%s algorithm error", TrustManagerFactory.class.getName());
        } catch (KeyStoreException e) {
            throw new JdbdMySQLException(e, "%s content error,cannot init %s."
                    , PropertyKey.trustCertificateKeyStoreUrl, TrustManagerFactory.class.getName());
        }
    }

    @Nullable
    private KeyManagerFactory tryObtainKeyManagerFactory() {
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(true);
            if (storePair == null) {
                return null;
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(storePair.getFirst(), storePair.getSecond());
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            // use default algorithm, never here.
            throw new JdbdMySQLException(e, "%s algorithm error", KeyManagerFactory.class.getName());
        } catch (KeyStoreException | UnrecoverableKeyException e) {
            throw new JdbdMySQLException(e, "%s content error,cannot init %s."
                    , PropertyKey.clientCertificateKeyStoreUrl, KeyManagerFactory.class.getName());
        }
    }

    @Nullable
    private Pair<KeyStore, char[]> tryObtainKeyStorePasswordPairForSsl(final boolean client) {
        // 1. below obtain three storeUrl,storeType,storePassword
        final PropertyKey storeUrlKey, storeTypeKey, passwordKey;
        final String defaultStoreUrlKey, defaultStoreTypeKey, defaultPasswordKey;
        if (client) {
            storeUrlKey = PropertyKey.clientCertificateKeyStoreUrl;
            storeTypeKey = PropertyKey.clientCertificateKeyStoreType;
            passwordKey = PropertyKey.clientCertificateKeyStorePassword;

            defaultStoreUrlKey = "javax.net.ssl.keyStore";
            defaultStoreTypeKey = "javax.net.ssl.keyStoreType";
            defaultPasswordKey = "javax.net.ssl.keyStorePassword";
        } else {
            storeUrlKey = PropertyKey.trustCertificateKeyStoreUrl;
            storeTypeKey = PropertyKey.trustCertificateKeyStoreType;
            passwordKey = PropertyKey.trustCertificateKeyStorePassword;

            defaultStoreUrlKey = "javax.net.ssl.trustStore";
            defaultStoreTypeKey = "javax.net.ssl.trustStoreType";
            defaultPasswordKey = "javax.net.ssl.trustStorePassword";
        }

        final Properties properties = this.properties;
        String storeUrl, storeType;
        storeUrl = properties.getProperty(storeUrlKey);
        storeType = properties.getProperty(storeTypeKey);

        if (!MySQLStringUtils.hasText(storeUrl)) {
            storeUrl = System.getProperty(defaultStoreUrlKey);
        }
        if (!MySQLStringUtils.hasText(storeType)) {
            storeType = System.getProperty(defaultStoreTypeKey);
        }

        if (!MySQLStringUtils.hasText(storeUrl) || !MySQLStringUtils.hasText(storeType)) {
            return null;
        }
        String storePwd = properties.getProperty(passwordKey);
        if (!MySQLStringUtils.hasText(storePwd)) {
            storePwd = System.getenv(defaultPasswordKey);
        }

        final char[] storePassword = (storePwd == null) ? new char[0] : storePwd.toCharArray();
        // 2. create and init KeyStore with three storeUrl,storeType,storePassword
        try (InputStream storeInput = new URL(storeUrl).openStream()) {
            try {
                KeyStore keyStore = KeyStore.getInstance(storeType);
                keyStore.load(storeInput, storePassword);
                return new Pair<>(keyStore, storePassword);
            } catch (KeyStoreException e) {
                throw new JdbdMySQLException(e, "%s[%s] error,cannot create %s.", storeTypeKey, storeType
                        , KeyStore.class.getName());
            } catch (NoSuchAlgorithmException | IOException | CertificateException e) {
                throw new JdbdMySQLException(e, "%s[%s] content error,cannot init %s."
                        , storeUrlKey, storeUrl, KeyStore.class.getName());
            }
        } catch (MalformedURLException e) {
            throw new JdbdMySQLException(e, "%s [%s] isn't url.", storeUrlKey, storeUrl);
        } catch (IOException e) {
            throw new JdbdMySQLException(e, "%s [%s] cannot open InputStream.", storeUrlKey, storeUrl);
        }
    }


}
