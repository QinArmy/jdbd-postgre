package io.jdbd.postgre.protocol.client;

import io.jdbd.meta.DataType;
import io.jdbd.result.RefCursor;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.protocol.DatabaseProtocol;
import reactor.core.publisher.Mono;

import java.util.Map;
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


    RefCursor refCursor(String name, Map<Option<?>, ?> optionMap, DatabaseSession session);

    Function<String, DataType> internalOrUserTypeFunc();

    boolean isNeedQueryUnknownType(Set<String> unknownTypeSet);

    Mono<Void> queryUnknownTypesIfNeed(Set<String> unknownTypeSet);

}
