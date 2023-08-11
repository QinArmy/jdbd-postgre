package io.jdbd.postgre.protocol;

import io.jdbd.vendor.env.HostInfo;
import io.jdbd.vendor.task.AbstractSslProviderBuilder;
import io.jdbd.vendor.task.SslMode;
import reactor.util.annotation.Nullable;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

public final class ReactorSslProviderBuilder extends AbstractSslProviderBuilder {

    public static ReactorSslProviderBuilder builder(HostInfo hostInfo) {
        return new ReactorSslProviderBuilder(hostInfo);
    }


    private ReactorSslProviderBuilder(HostInfo hostInfo) {
        super(hostInfo);
    }

    @Nullable
    @Override
    protected final StoreProps getStorePropsForKey() {
        String url = this.properties.get(PgKey0.keyStoreType);
        StoreProps props;
        if (url == null) {
            props = null;
        } else {
            props = new StoreProps(this.properties.get(PgKey0.keyStoreUrl)
                    , url, this.properties.get(PgKey0.keyStorePassword));
        }
        return props;
    }

    @Nullable
    @Override
    protected final StoreProps getStorePropsForTrust() {
        String url = this.properties.get(PgKey0.trustStoreUrl);
        StoreProps props;
        if (url == null) {
            props = null;
        } else {
            props = new StoreProps(this.properties.get(PgKey0.trustStoreType)
                    , url, this.properties.get(PgKey0.trustStorePassword));
        }
        return props;
    }

    @Override
    protected final boolean isFallbackToSystemKeyStore() {
        return this.properties.getOrDefault(PgKey0.fallbackToSystemKeyStore, Boolean.class);
    }

    @Override
    protected final SslMode getSslMode() {
        return this.properties.getOrDefault(PgKey0.sslmode, SslMode.class);
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
