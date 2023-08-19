package io.jdbd.postgre.protocol.client;

import io.netty.handler.ssl.SslHandler;

final class ReactorSslProviderBuilder {

    static ReactorSslProviderBuilder builder() {
        return new ReactorSslProviderBuilder();
    }

    private ReactorSslProviderBuilder() {
    }


    private TaskAdjutant adjutant;

    public final TaskAdjutant adjutant() {
        return adjutant;
    }


    public final SslHandler buildSslHandler() {
        return null;
    }

}
