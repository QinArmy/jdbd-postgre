package io.jdbd.postgre.protocol.client;

import io.jdbd.result.RefCursor;
import io.jdbd.session.DatabaseSession;
import io.jdbd.session.Option;
import io.jdbd.vendor.protocol.DatabaseProtocol;

import java.util.Map;

public interface PgProtocol extends DatabaseProtocol {


    RefCursor refCursor(String name, Map<Option<?>, ?> optionMap, DatabaseSession session);


}
