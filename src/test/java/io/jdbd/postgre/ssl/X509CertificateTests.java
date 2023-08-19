package io.jdbd.postgre.ssl;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpClient;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class X509CertificateTests {

    private static final Logger LOG = LoggerFactory.getLogger(X509CertificateTests.class);


    @Test
    public void certificateChain() throws Exception {
        TcpClient.create()
                .host("www.baidu.com")
                .port(443)
                .secure(createSslProvider())
                .connectNow()
        ;
    }


    private SslProvider createSslProvider() throws Exception {
        SslContext context = SslContextBuilder.forClient()
                .trustManager(createTrustManager())
                .build();

        return SslProvider.builder()
                .sslContext(context)
                .build();
    }


    private TrustManager createTrustManager() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String authType)
                    throws CertificateException {
                LOG.debug("client trusted authType:{}", authType);
                for (X509Certificate certificate : x509Certificates) {
                    certificate.checkValidity();
                    LOG.debug("client certificate number:{}", certificate.getSerialNumber());
                }

            }

            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String authType)
                    throws CertificateException {
                LOG.debug("server trusted authType:{}", authType);
                for (X509Certificate certificate : x509Certificates) {
                    certificate.checkValidity();
                    LOG.debug("server certificate number:{}, domain names:{}"
                            , certificate.getSerialNumber()
                            , certificate.getSubjectAlternativeNames());

                }

            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };

    }


}
