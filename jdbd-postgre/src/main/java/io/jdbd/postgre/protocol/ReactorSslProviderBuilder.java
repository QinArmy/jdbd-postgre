package io.jdbd.postgre.protocol;

import io.jdbd.postgre.config.PGKey;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.task.AbstractSslProviderBuilder;
import io.jdbd.vendor.task.SslMode;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public final class ReactorSslProviderBuilder extends AbstractSslProviderBuilder<PGKey> {

    public static ReactorSslProviderBuilder builder(HostInfo<PGKey> hostInfo) {
        return new ReactorSslProviderBuilder(hostInfo);
    }


    private ReactorSslProviderBuilder(HostInfo<PGKey> hostInfo) {
        super(hostInfo);
    }

    @Nullable
    @Override
    protected final StoreProps getStorePropsForKey() {
        String url = this.properties.getProperty(PGKey.keyStoreType);
        StoreProps props;
        if (url == null) {
            props = null;
        } else {
            props = new StoreProps(this.properties.getProperty(PGKey.keyStoreUrl)
                    , url, this.properties.getProperty(PGKey.keyStorePassword));
        }
        return props;
    }

    @Nullable
    @Override
    protected final StoreProps getStorePropsForTrust() {
        String url = this.properties.getProperty(PGKey.trustStoreUrl);
        StoreProps props;
        if (url == null) {
            props = null;
        } else {
            props = new StoreProps(this.properties.getProperty(PGKey.trustStoreType)
                    , url, this.properties.getProperty(PGKey.trustStorePassword));
        }
        return props;
    }

    @Override
    protected final boolean isFallbackToSystemKeyStore() {
        return this.properties.getOrDefault(PGKey.fallbackToSystemKeyStore, Boolean.class);
    }

    @Override
    protected final SslMode getSslMode() {
        return this.properties.getOrDefault(PGKey.sslmode, SslMode.class);
    }

    @Override
    protected final TrustManagerFactory mapTrustManagerFactory(TrustManagerFactory factory) {
        return super.mapTrustManagerFactory(factory);
    }

    @Override
    protected final KeyManagerFactory mapKeyManagerFactory(KeyManagerFactory factory) {
        return super.mapKeyManagerFactory(factory);
    }


}
