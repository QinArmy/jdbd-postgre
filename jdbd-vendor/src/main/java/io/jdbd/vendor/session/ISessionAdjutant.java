package io.jdbd.vendor.session;

import io.jdbd.session.DatabaseSession;
import io.jdbd.session.DatabaseSessionFactory;
import io.jdbd.vendor.conf.JdbcUrl;
import io.jdbd.vendor.conf.PropertyKey;
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

    JdbcUrl getJdbcUrl();

    EventLoopGroup getEventLoopGroup();


    boolean isSameFactory(DatabaseSessionFactory factory);


}
