package io.jdbd.postgre.protocol;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public final class PgX509TrustManagerWrapper implements X509TrustManager {


    private PgX509TrustManagerWrapper() {

    }


    @Override
    public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {

    }

    @Override
    public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {

    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }


}
