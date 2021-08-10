package io.jdbd.postgre.util;

import io.jdbd.vendor.util.Geometries;

public abstract class PgGeometries extends Geometries {

    /**
     * @param textValue format:[ ( x1 , y1 ) , ( x2 , y2 ) ]
     * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Line Segments</a>
     */
    public static byte[] lineSegmentToWbk(final String textValue, final boolean bigEndian) {


        return null;
    }

}
