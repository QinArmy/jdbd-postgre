package io.jdbd.postgre.session;

import io.jdbd.postgre.config.PgUrl;
import io.jdbd.vendor.session.ISessionAdjutant;

public interface SessionAdjutant extends ISessionAdjutant {


    @Override
    PgUrl jdbcUrl();


}
