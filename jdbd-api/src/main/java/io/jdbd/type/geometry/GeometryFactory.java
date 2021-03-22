package io.jdbd.type.geometry;

import java.nio.file.Path;
import java.util.List;

public interface GeometryFactory {

       Point point(double x, double y);

       Point pointFromWkb(byte[] wkb, int offset);

       Point pointFromWkt(String wkt, int offset);

       LineString lineString(List<Point> pointList);

       LineString lineStringFromWkb(byte[] wkb, int offset);

       LineString lineStringFromWkt(String wkt, int offset);

       LineString lineStringFromWkbPath(Path path, long offset);


}
