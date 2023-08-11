package io.jdbd.postgre.protocol.client;

import io.jdbd.result.RefCursor;
import io.jdbd.session.DatabaseSession;
import io.jdbd.vendor.protocol.DatabaseProtocol;

public interface PgProtocol extends DatabaseProtocol {


    RefCursor refCursor(String name, DatabaseSession session);


}
