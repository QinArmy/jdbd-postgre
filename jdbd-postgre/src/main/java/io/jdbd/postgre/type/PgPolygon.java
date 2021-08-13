package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Point;
import reactor.core.publisher.Flux;

import java.util.Objects;

/**
 * <p>
 * JDBD statement bind method not don't support this type,only supported by {@link io.jdbd.result.ResultRow}.
 * </p>
 */
public final class PgPolygon implements PGobject {

    /**
     * <p>
     * don't check textValue content.
     * </p>
     *
     * @param textValue format : ( ( x1 , y1 ) , ... , ( xn , yn ) )
     */
    public static PgPolygon create(String textValue) {
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
        return this.textValue.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof PgPolygon) {
            PgPolygon p = (PgPolygon) obj;
            match = this.textValue.equals(p.textValue);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return this.textValue;
    }


}
