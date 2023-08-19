package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.result.RefCursor;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import reactor.core.publisher.Mono;

import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * <p>
 * This interface provide the ability that use postgre protocol for postgre {@link DatabaseSession}.
 * </p>
 *
 * @since 1.0
 */
public interface PgProtocol extends DatabaseProtocol {

    Option<String> DATE_STYLE = Option.from("PG_DATE_STYLE", String.class);

    Option<String> INTERNAL_STYLE = Option.from("PG_INTERNAL_STYLE", String.class);

    Option<Locale> MONEY_LOCAL = Option.from("PG_MONEY_LOCAL", Locale.class);


    RefCursor refCursor(String name, Function<Option<?>, ?> optionFunc, DatabaseSession session);

    Function<String, DataType> internalOrUserTypeFunc();

    boolean isNeedQueryUnknownType(Set<String> unknownTypeSet);

    Mono<Void> queryUnknownTypesIfNeed(Set<String> unknownTypeSet);

}
