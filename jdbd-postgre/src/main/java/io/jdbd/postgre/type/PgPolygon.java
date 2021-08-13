package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Point;
import reactor.core.publisher.Flux;

import java.util.Objects;

/**
 * <p>
 * JDBD statement bind method not don't support this type,only supported by {@link io.jdbd.result.ResultRow}.
 * </p>
 *
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-POLYGON">Polygons</a>
 */
@Deprecated
final class PgPolygon implements PGobject {

    /**
     * <p>
     * don't check textValue content.
     * </p>
     *
     * @param textValue format : ( ( x1 , y1 ) , ... , ( xn , yn ) )
     */
    public static PgPolygon wrap(String textValue) {
        if (!textValue.startsWith("(") || !textValue.endsWith(")")) {
            throw PgGeometries.createGeometricFormatError(textValue);
        }
        return new PgPolygon(textValue);
    }

    private final String textValue;

    private PgPolygon(String textValue) {
        this.textValue = Objects.requireNonNull(textValue, "textValue");
    }


    public final Flux<Point> toPoints() {
        return Flux.create(sink -> PgGeometries.polygonToPoints(this.textValue, sink));
    }


    @Override
    public final int hashCode() {
        // this class only wrap Polygon for toPoints()  method
        return super.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        // this class only wrap Polygon for toPoints()  method
        return super.equals(obj);
    }

    @Override
    public final String toString() {
        return this.textValue;
    }


}
