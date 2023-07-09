package io.jdbd.type;

/**
 * This interface representing geometry point.
 *
 * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
 * @see <a href="https://portal.ogc.org/files/?artifact_id=25355">PDF download</a>
 */
public interface Point {

    double getX();

    double getY();

    /**
     * override {@link Object#hashCode()}
     */
    @Override
    int hashCode();

    /**
     * override {@link Object#equals(Object)}
     */
    @Override
    boolean equals(Object obj);

    /**
     * override {@link Object#toString()}
     *
     * @return WKT
     */
    @Override
    String toString();


    static Point from(double x, double y) {
        return JdbdTypes.point(x, y);
    }

}
