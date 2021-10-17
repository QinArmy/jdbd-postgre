package io.jdbd.postgre.session;

import io.jdbd.postgre.config.PostgreUrl;
import io.jdbd.vendor.session.ISessionAdjutant;

public interface SessionAdjutant extends ISessionAdjutant {


    @Override
    PostgreUrl getJdbcUrl();


}
