package io.jdbd.postgre.session;

import io.jdbd.postgre.config.PGKey;
import io.jdbd.postgre.config.PostgreHost;
import io.jdbd.postgre.config.PostgreUrl;
import io.jdbd.vendor.session.ISessionAdjutant;

public interface SessionAdjutant extends ISessionAdjutant<PGKey, PostgreHost> {


    @Override
    PostgreUrl obtainUrl();
}
