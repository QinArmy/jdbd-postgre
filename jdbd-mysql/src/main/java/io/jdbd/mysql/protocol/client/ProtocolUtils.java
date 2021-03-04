package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.JdbdMySQLException;
import io.jdbd.mysql.protocol.conf.PropertyKey;
import io.jdbd.mysql.util.MySQLStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class ProtocolUtils {

    protected ProtocolUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(ProtocolUtils.class);


    static final String TLSv1 = "TLSv1";
    static final String TLSv1_1 = "TLSv1.1";
    static final String TLSv1_2 = "TLSv1.2";
    static final String TLSv1_3 = "TLSv1.3";

    /** a unmodifiable list */
    static final List<String> MYSQL_RESTRICTED_CIPHER_SUBSTR_LIST = createMySQLRestrictedCipherSubStrList();

    /** a unmodifiable list */
    static final List<String> CLIENT_SUPPORT_TLS_PROTOCOL_LIST = createClientSupportTlsProtocolList();

    /** a unmodifiable list */
    static final List<String> CLIENT_SUPPORT_TLS_CIPHER_LIST = createClientSupportCipherSuitList();

    /**
     * @return a unmodifiable list
     */
    private static List<String> createClientSupportCipherSuitList() {
        List<String> list = createMySQLSupportTlsCipherSuitList();
        list.retainAll(createJdkSupportCipherSuitList());
        return Collections.unmodifiableList(list);
    }

    /**
     * @return a unmodifiable list
     */
    private static List<String> createClientSupportTlsProtocolList() {
        List<String> list = createMySQLSupportTlsProtocolList();
        list.retainAll(createJdkSupportTlsProtocolList());
        return Collections.unmodifiableList(list);
    }

    /**
     * @return a unmodifiable list
     */
    private static List<String> createMySQLRestrictedCipherSubStrList() {
        List<String> list = new ArrayList<>(8);
        // Unacceptable TLS Ciphers
        list.add("_ANON_");
        list.add("_NULL_");
        list.add("_EXPORT");
        list.add("_MD5");

        list.add("_DES");
        list.add("_RC2_");
        list.add("_RC4_");
        list.add("_PSK_");
        return Collections.unmodifiableList(list);
    }


    /**
     * @return a modifiable list
     */
    private static List<String> createMySQLSupportTlsProtocolList() {
        List<String> list = new ArrayList<>(4);
        list.add(TLSv1);
        list.add(TLSv1_1);
        list.add(TLSv1_2);
        list.add(TLSv1_3);
        return list;
    }

    /**
     * @return a modifiable list
     */
    static List<String> createMySQLSupportTlsCipherSuitList() {
        List<String> list = new ArrayList<>();

        // Mandatory TLS Ciphers");

        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");


        // Approved TLS Ciphers");
        list.add("TLS_AES_128_GCM_SHA256");
        list.add("TLS_AES_256_GCM_SHA384");
        list.add("TLS_CHACHA20_POLY1305_SHA256");
        list.add("TLS_AES_128_CCM_SHA256");

        list.add("TLS_AES_128_CCM_8_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DHE_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DHE_DSS_WITH_AES_128_GCM_SHA256");

        list.add("TLS_DHE_DSS_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256");

        list.add("TLS_DH_DSS_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DH_DSS_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384");

        list.add("TLS_DH_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DH_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384");


        // Deprecated TLS Ciphers");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");

        list.add("TLS_DHE_DSS_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DHE_DSS_WITH_AES_256_CBC_SHA256");
        list.add("TLS_DHE_RSA_WITH_AES_256_CBC_SHA256");
        list.add("TLS_DHE_RSA_WITH_AES_128_CBC_SHA256");

        list.add("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA");

        list.add("TLS_DHE_DSS_WITH_AES_128_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_DHE_DSS_WITH_AES_256_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_AES_256_CBC_SHA");

        list.add("TLS_DH_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DH_RSA_WITH_AES_256_CBC_SHA256");

        list.add("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_DH_DSS_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_DH_DSS_WITH_AES_128_CBC_SHA");

        list.add("TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_AES_256_CBC_SHA256");

        list.add("TLS_DH_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDH_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_DH_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA");

        list.add("TLS_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_RSA_WITH_AES_256_CBC_SHA256");

        list.add("TLS_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_RSA_WITH_CAMELLIA_256_CBC_SHA");
        list.add("TLS_RSA_WITH_CAMELLIA_128_CBC_SHA");

        list.add("TLS_DH_DSS_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DH_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA");

        list.add("TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA");

        list.add("TLS_RSA_WITH_3DES_EDE_CBC_SHA");

        return list;
    }


    /**
     * @return a modifiable list
     * @see <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext">JDK TLS protocls</a>
     */
    private static List<String> createJdkSupportTlsProtocolList() {
        List<String> mySqlSupportTlsProtocolList = createMySQLSupportTlsProtocolList();
        List<String> list = new ArrayList<>(mySqlSupportTlsProtocolList.size());
        for (String protocol : mySqlSupportTlsProtocolList) {
            try {
                SSLContext.getInstance(protocol);
                list.add(protocol);
            } catch (NoSuchAlgorithmException e) {
                LOG.debug("{} unsupported by JDK,ignore.", protocol);
            }
        }
        return list;
    }


    /**
     * @return a modifiable list
     * @see <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#ciphersuites">JDK Cipher suit</a>
     */
    private static List<String> createJdkSupportCipherSuitList() {
        List<String> list = new ArrayList<>();

        list.add("SSL_DH_anon_EXPORT_WITH_DES40_CBC_SHA");
        list.add("SSL_DH_anon_EXPORT_WITH_RC4_40_MD5");
        list.add("SSL_DH_anon_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DH_anon_WITH_AES_128_CBC_SHA");

        list.add("TLS_DH_anon_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DH_anon_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DH_anon_WITH_AES_256_CBC_SHA");
        list.add("TLS_DH_anon_WITH_AES_256_CBC_SHA256");

        list.add("TLS_DH_anon_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA");
        list.add("TLS_DH_anon_WITH_CAMELLIA_128_CBC_SHA256");
        list.add("TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA");

        list.add("TLS_DH_anon_WITH_CAMELLIA_256_CBC_SHA256");
        list.add("SSL_DH_anon_WITH_DES_CBC_SHA");
        list.add("SSL_DH_anon_WITH_RC4_128_MD5");
        list.add("TLS_DH_anon_WITH_SEED_CBC_SHA");

        list.add("SSL_DH_DSS_EXPORT_WITH_DES40_CBC_SHA");
        list.add("SSL_DH_DSS_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_AES_128_CBC_SHA");
        // list.add("TLS_DH_DSS_WITH_AES_128_CBC_SHA256");

        //list.add("TLS_DH_DSS_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DH_DSS_WITH_AES_256_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_AES_256_CBC_SHA256");
        // list.add("TLS_DH_DSS_WITH_AES_256_GCM_SHA384");

        list.add("TLS_DH_DSS_WITH_CAMELLIA_128_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_CAMELLIA_128_CBC_SHA256");
        list.add("TLS_DH_DSS_WITH_CAMELLIA_256_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_CAMELLIA_256_CBC_SHA256");
        list.add("SSL_DH_DSS_WITH_DES_CBC_SHA");

        list.add("TLS_DH_DSS_WITH_SEED_CBC_SHA");
        list.add("SSL_DH_RSA_EXPORT_WITH_DES40_CBC_SHA");
        list.add("SSL_DH_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DH_RSA_WITH_AES_128_CBC_SHA");

        //  list.add("TLS_DH_RSA_WITH_AES_128_CBC_SHA256");
        // list.add("TLS_DH_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DH_RSA_WITH_AES_256_CBC_SHA");
        // list.add("TLS_DH_RSA_WITH_AES_256_CBC_SHA256");

        //list.add("TLS_DH_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DH_RSA_WITH_CAMELLIA_128_CBC_SHA");
        list.add("TLS_DH_RSA_WITH_CAMELLIA_128_CBC_SHA256");
        list.add("TLS_DH_RSA_WITH_CAMELLIA_256_CBC_SHA");

        list.add("TLS_DH_RSA_WITH_CAMELLIA_256_CBC_SHA256");
        list.add("SSL_DH_RSA_WITH_DES_CBC_SHA");
        list.add("TLS_DH_RSA_WITH_SEED_CBC_SHA");
        list.add("SSL_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA");

        list.add("SSL_DHE_DSS_EXPORT1024_WITH_DES_CBC_SHA");
        list.add("SSL_DHE_DSS_EXPORT1024_WITH_RC4_56_SHA");
        list.add("SSL_DHE_DSS_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DHE_DSS_WITH_AES_128_CBC_SHA");

        list.add("TLS_DHE_DSS_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DHE_DSS_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DHE_DSS_WITH_AES_256_CBC_SHA");
        list.add("TLS_DHE_DSS_WITH_AES_256_CBC_SHA256");

        list.add("TLS_DHE_DSS_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA");
        list.add("TLS_DHE_DSS_WITH_CAMELLIA_128_CBC_SHA256");
        list.add("TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA");

        list.add("TLS_DHE_DSS_WITH_CAMELLIA_256_CBC_SHA256");
        list.add("SSL_DHE_DSS_WITH_DES_CBC_SHA");
        list.add("SSL_DHE_DSS_WITH_RC4_128_SHA");
        list.add("TLS_DHE_DSS_WITH_SEED_CBC_SHA");

        list.add("TLS_DHE_PSK_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DHE_PSK_WITH_AES_128_CBC_SHA");
        list.add("TLS_DHE_PSK_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DHE_PSK_WITH_AES_128_GCM_SHA256");

        list.add("TLS_DHE_PSK_WITH_AES_256_CBC_SHA");
        list.add("TLS_DHE_PSK_WITH_AES_256_CBC_SHA384");
        list.add("TLS_DHE_PSK_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DHE_PSK_WITH_NULL_SHA");

        list.add("TLS_DHE_PSK_WITH_NULL_SHA256");
        list.add("TLS_DHE_PSK_WITH_NULL_SHA384");
        list.add("TLS_DHE_PSK_WITH_RC4_128_SHA");
        list.add("SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA");

        list.add("SSL_DHE_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DHE_RSA_WITH_AES_128_GCM_SHA256");

        list.add("TLS_DHE_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_AES_256_CBC_SHA256");
        list.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA");

        list.add("TLS_DHE_RSA_WITH_CAMELLIA_128_CBC_SHA256");
        list.add("TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_CAMELLIA_256_CBC_SHA256");
        list.add("SSL_DHE_RSA_WITH_DES_CBC_SHA");

        list.add("TLS_DHE_RSA_WITH_SEED_CBC_SHA");
        list.add("TLS_ECDH_anon_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDH_anon_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDH_anon_WITH_AES_256_CBC_SHA");

        list.add("TLS_ECDH_anon_WITH_NULL_SHA");
        list.add("TLS_ECDH_anon_WITH_RC4_128_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA");

        list.add("TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384");

        list.add("TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDH_ECDSA_WITH_NULL_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_RC4_128_SHA");
        list.add("TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA");

        list.add("TLS_ECDH_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA");

        list.add("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDH_RSA_WITH_NULL_SHA");
        list.add("TLS_ECDH_RSA_WITH_RC4_128_SHA");

        list.add("TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");

        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDHE_ECDSA_WITH_NULL_SHA");

        list.add("TLS_ECDHE_ECDSA_WITH_RC4_128_SHA");
        list.add("TLS_ECDHE_PSK_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDHE_PSK_WITH_AES_128_CBC_SHA256");

        list.add("TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDHE_PSK_WITH_AES_256_CBC_SHA384");
        list.add("TLS_ECDHE_PSK_WITH_NULL_SHA");
        list.add("TLS_ECDHE_PSK_WITH_NULL_SHA256");

        list.add("TLS_ECDHE_PSK_WITH_NULL_SHA384");
        list.add("TLS_ECDHE_PSK_WITH_RC4_128_SHA");
        list.add("TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");

        list.add("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");

        list.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDHE_RSA_WITH_NULL_SHA");
        list.add("TLS_ECDHE_RSA_WITH_RC4_128_SHA");
        list.add("TLS_EMPTY_RENEGOTIATION_INFO_SCSV *");

        list.add("SSL_FORTEZZA_DMS_WITH_FORTEZZA_CBC_SHA");
        list.add("SSL_FORTEZZA_DMS_WITH_NULL_SHA");
        list.add("TLS_KRB5_EXPORT_WITH_DES_CBC_40_MD5");
        list.add("TLS_KRB5_EXPORT_WITH_DES_CBC_40_SHA");

        list.add("TLS_KRB5_EXPORT_WITH_RC2_CBC_40_MD5");
        list.add("TLS_KRB5_EXPORT_WITH_RC2_CBC_40_SHA");
        list.add("TLS_KRB5_EXPORT_WITH_RC4_40_MD5");
        list.add("TLS_KRB5_EXPORT_WITH_RC4_40_SHA");

        list.add("TLS_KRB5_WITH_3DES_EDE_CBC_MD5");
        list.add("TLS_KRB5_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_KRB5_WITH_DES_CBC_MD5");
        list.add("TLS_KRB5_WITH_DES_CBC_SHA");

        list.add("TLS_KRB5_WITH_IDEA_CBC_MD5");
        list.add("TLS_KRB5_WITH_IDEA_CBC_SHA");
        list.add("TLS_KRB5_WITH_RC4_128_MD5");
        list.add("TLS_KRB5_WITH_RC4_128_SHA");

        list.add("TLS_PSK_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_PSK_WITH_AES_128_CBC_SHA");
        list.add("TLS_PSK_WITH_AES_128_CBC_SHA256");
        list.add("TLS_PSK_WITH_AES_128_GCM_SHA256");

        list.add("TLS_PSK_WITH_AES_256_CBC_SHA");
        list.add("TLS_PSK_WITH_AES_256_CBC_SHA384");
        list.add("TLS_PSK_WITH_AES_256_GCM_SHA384");
        list.add("TLS_PSK_WITH_NULL_SHA");

        list.add("TLS_PSK_WITH_NULL_SHA256");
        list.add("TLS_PSK_WITH_NULL_SHA384");
        list.add("TLS_PSK_WITH_RC4_128_SHA");
        list.add("SSL_RSA_EXPORT_WITH_DES40_CBC_SHA");

        list.add("SSL_RSA_EXPORT_WITH_RC2_CBC_40_MD5");
        list.add("SSL_RSA_EXPORT_WITH_RC4_40_MD5");
        list.add("SSL_RSA_EXPORT1024_WITH_DES_CBC_SHA");
        list.add("SSL_RSA_EXPORT1024_WITH_RC4_56_SHA");

        list.add("SSL_RSA_FIPS_WITH_3DES_EDE_CBC_SHA");
        list.add("SSL_RSA_FIPS_WITH_DES_CBC_SHA");
        list.add("TLS_RSA_PSK_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_RSA_PSK_WITH_AES_128_CBC_SHA");

        list.add("TLS_RSA_PSK_WITH_AES_128_CBC_SHA256");
        list.add("TLS_RSA_PSK_WITH_AES_128_GCM_SHA256");
        list.add("TLS_RSA_PSK_WITH_AES_256_CBC_SHA");
        list.add("TLS_RSA_PSK_WITH_AES_256_CBC_SHA384");

        list.add("TLS_RSA_PSK_WITH_AES_256_GCM_SHA384");
        list.add("TLS_RSA_PSK_WITH_NULL_SHA");
        list.add("TLS_RSA_PSK_WITH_NULL_SHA256");
        list.add("TLS_RSA_PSK_WITH_NULL_SHA384");

        list.add("TLS_RSA_PSK_WITH_RC4_128_SHA");
        list.add("SSL_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_RSA_WITH_AES_128_CBC_SHA256");

        list.add("TLS_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_RSA_WITH_AES_256_CBC_SHA256");
        list.add("TLS_RSA_WITH_AES_256_GCM_SHA384");

        list.add("TLS_RSA_WITH_CAMELLIA_128_CBC_SHA");
        list.add("TLS_RSA_WITH_CAMELLIA_128_CBC_SHA256");
        list.add("TLS_RSA_WITH_CAMELLIA_256_CBC_SHA");
        list.add("TLS_RSA_WITH_CAMELLIA_256_CBC_SHA256");

        list.add("SSL_RSA_WITH_DES_CBC_SHA");
        list.add("SSL_RSA_WITH_IDEA_CBC_SHA");
        list.add("SSL_RSA_WITH_NULL_MD5");
        list.add("SSL_RSA_WITH_NULL_SHA");

        list.add("TLS_RSA_WITH_NULL_SHA256");
        list.add("SSL_RSA_WITH_RC4_128_MD5");
        list.add("SSL_RSA_WITH_RC4_128_SHA");
        list.add("TLS_RSA_WITH_SEED_CBC_SHA");

        list.add("TLS_SRP_SHA_DSS_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_SRP_SHA_DSS_WITH_AES_128_CBC_SHA");
        list.add("TLS_SRP_SHA_DSS_WITH_AES_256_CBC_SHA");
        list.add("TLS_SRP_SHA_RSA_WITH_3DES_EDE_CBC_SHA");

        list.add("TLS_SRP_SHA_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_SRP_SHA_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_SRP_SHA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_SRP_SHA_WITH_AES_128_CBC_SHA");
        list.add("TLS_SRP_SHA_WITH_AES_256_CBC_SHA");

        return list;
    }

    static String buildSetVariableCommand(String pairString) {
        List<String> pairList = MySQLStringUtils.split(pairString, ",;", "\"'(", "\"')");
        StringBuilder builder = new StringBuilder("SET ");
        int index = 0;
        for (String pair : pairList) {
            if (!pair.contains("=")) {
                throw new JdbdMySQLException("Property sessionVariables format error,please check it.");
            }
            String lower = pair.toLowerCase();
            if (lower.contains(ClientConnectionProtocolImpl.CHARACTER_SET_RESULTS)
                    || lower.contains(ClientConnectionProtocolImpl.CHARACTER_SET_CLIENT)
                    || lower.contains(ClientConnectionProtocolImpl.COLLATION_CONNECTION)) {
                throw new JdbdMySQLException(
                        "Below three session variables[%s,%s,%s] must specified by below three properties[%s,%s,%s]."
                        , ClientConnectionProtocolImpl.CHARACTER_SET_CLIENT
                        , ClientConnectionProtocolImpl.CHARACTER_SET_RESULTS
                        , ClientConnectionProtocolImpl.COLLATION_CONNECTION
                        , PropertyKey.characterEncoding
                        , PropertyKey.characterSetResults
                        , PropertyKey.connectionCollation);

            }
            if (index > 0) {
                builder.append(",");
            }
            if (!pair.contains("@")) {
                builder.append("@@SESSION.");
            }
            builder.append(pair);
            index++;
        }
        return builder.toString();
    }


}
