package io.jdbd.vendor.session;

import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.env.JdbcUrl;
import io.jdbd.vendor.env.PropertyKey;
import io.netty.channel.EventLoopGroup;

/**
 * <p>
 * This interface help {@link DatabaseSession} obtain session context that store in {@link DatabaseSessionFactory}.
 * </p>
 *
 * @param <K> {@link PropertyKey} type.
 * @see DatabaseSessionFactory
 */
public interface ISessionAdjutant {

    JdbcUrl jdbcUrl();

    EventLoopGroup eventLoopGroup();


    boolean isSameFactory(DatabaseSessionFactory factory);


}
