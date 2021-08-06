package io.jdbd.postgre.protocol;

import io.jdbd.postgre.config.PgKey;
import io.jdbd.vendor.conf.HostInfo;
import io.jdbd.vendor.task.AbstractSslProviderBuilder;
import io.jdbd.vendor.task.SslMode;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public final class ReactorSslProviderBuilder extends AbstractSslProviderBuilder<PgKey> {

    public static ReactorSslProviderBuilder builder(HostInfo<PgKey> hostInfo) {
        return new ReactorSslProviderBuilder(hostInfo);
    }


    private ReactorSslProviderBuilder(HostInfo<PgKey> hostInfo) {
        super(hostInfo);
    }

    @Nullable
    @Override
    protected final StoreProps getStorePropsForKey() {
        String url = this.properties.getProperty(PgKey.keyStoreType);
        StoreProps props;
        if (url == null) {
            props = null;
        } else {
            props = new StoreProps(this.properties.getProperty(PgKey.keyStoreUrl)
                    , url, this.properties.getProperty(PgKey.keyStorePassword));
        }
        return props;
    }

    @Nullable
    @Override
    protected final StoreProps getStorePropsForTrust() {
        String url = this.properties.getProperty(PgKey.trustStoreUrl);
        StoreProps props;
        if (url == null) {
            props = null;
        } else {
            props = new StoreProps(this.properties.getProperty(PgKey.trustStoreType)
                    , url, this.properties.getProperty(PgKey.trustStorePassword));
        }
        return props;
    }

    @Override
    protected final boolean isFallbackToSystemKeyStore() {
        return this.properties.getOrDefault(PgKey.fallbackToSystemKeyStore, Boolean.class);
    }

    @Override
    protected final SslMode getSslMode() {
        return this.properties.getOrDefault(PgKey.sslmode, SslMode.class);
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
