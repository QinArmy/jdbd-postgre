package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Point;
import io.jdbd.type.geometry.SmallLineString;
import io.jdbd.vendor.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @see Geometries
 */
@Test(groups = {Groups.GEOMETRIES})
public class GeometriesSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(GeometriesSuiteTests.class);


    /**
     * @see Geometries#pointFromWkb(byte[], int)
     */
    @Test
    public void pointFromWkb() {
        double x, y;
        x = 0.0;
        y = 0.0;

        Point point, result;
        byte[] wkbBytes;

        point = Geometries.point(x, y);
        wkbBytes = point.asWkbBytes(true);
        result = Geometries.pointFromWkb(wkbBytes, 0);
        assertEquals(result, point, "result");

        wkbBytes = result.asWkbBytes(false);
        result = Geometries.pointFromWkb(wkbBytes, 0);
        assertEquals(result, point, "result");


        x = Double.MAX_VALUE;
        y = Double.MIN_VALUE;

        point = Geometries.point(x, y);
        wkbBytes = point.asWkbBytes(false);
        result = Geometries.pointFromWkb(wkbBytes, 0);
        assertEquals(result, point, "result");

        wkbBytes = result.asWkbBytes(true);
        result = Geometries.pointFromWkb(wkbBytes, 0);
        assertEquals(result, point, "result");
    }

    /**
     * @see Geometries#pointFromWkt(String)
     */
    @Test
    public void pointFromWkt() {
        String wkt;
        Point point, result;

        wkt = "POINT(0 0)";
        point = Geometries.point(0, 0);
        result = Geometries.pointFromWkt(wkt);
        assertEquals(result, point, "result");

        wkt = point.asWkt();
        result = Geometries.pointFromWkt(wkt);
        assertEquals(result, point, "result");


        wkt = String.format("POINT(%s %s)", Double.MAX_VALUE, Double.MIN_VALUE);
        point = Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE);
        result = Geometries.pointFromWkt(wkt);
        assertEquals(result, point, "result");

        wkt = point.asWkt();
        result = Geometries.pointFromWkt(wkt);
        assertEquals(result, point, "result");


    }

    /**
     * @see Geometries#lineStringFromWkb(byte[], int)
     */
    @Test(dependsOnMethods = {"pointFromWkb"})
    public void lineStringFromWkb() {
        SmallLineString lineString, result;
        List<Point> pointList;
        byte[] wkbBytes;

        pointList = new ArrayList<>(2);
        pointList.add(Geometries.point(0, 0));
        pointList.add(Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE));

        lineString = Geometries.lineString(pointList, false);

        wkbBytes = lineString.asWkbBytes(true);
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

        wkbBytes = result.asWkbBytes(false);
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

        pointList = new ArrayList<>(4);
        pointList.add(Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE));
        pointList.add(Geometries.point(0, 0));
        pointList.add(Geometries.point(-1, 3));
        pointList.add(Geometries.point(-1, -1));

        lineString = Geometries.lineString(pointList, false);

        wkbBytes = lineString.asWkbBytes(true);
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

        wkbBytes = result.asWkbBytes(false);
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

    }

    /**
     * @see Geometries#lineStringFromWkt(String)
     */
    @Test
    public void lineFromWkt() {
        String wkt, resultWkt;
        SmallLineString lineString, result;
        List<Point> pointList;

        wkt = String.format("LINESTRING(0 0,%s %s)", Double.MIN_VALUE, Double.MIN_VALUE);
        lineString = Geometries.lineStringFromWkt(wkt);
        resultWkt = lineString.asWkt();
        result = Geometries.lineStringFromWkt(resultWkt);
        assertEquals(result, lineString, "resultWkt");

        pointList = lineString.pointList();
        assertEquals(pointList.size(), 2, "pointList size");
        assertEquals(pointList.get(0), Geometries.point(0, 0), "zero point");
        assertEquals(pointList.get(1), Geometries.point(Double.MIN_VALUE, Double.MIN_VALUE), "POINT(MIN_VALUE MIN_VALUE)");


        wkt = String.format("LINESTRING(1 1,2 2,3 3,-1 -5,%s %s)", Double.MIN_VALUE, Double.MIN_VALUE);
        lineString = Geometries.lineStringFromWkt(wkt);
        resultWkt = lineString.asWkt();
        result = Geometries.lineStringFromWkt(resultWkt);
        assertEquals(result, lineString, "resultWkt");

        pointList = lineString.pointList();
        assertEquals(pointList.size(), 5, "pointList size");

        assertEquals(pointList.get(0), Geometries.point(1, 1), "POINT(1 1)");
        assertEquals(pointList.get(1), Geometries.point(2, 2), "POINT(2 2)");
        assertEquals(pointList.get(2), Geometries.point(3, 3), "POINT(3 3)");
        assertEquals(pointList.get(3), Geometries.point(-1, -5), "POINT(-1 -5)");

        assertEquals(pointList.get(4), Geometries.point(Double.MIN_VALUE, Double.MIN_VALUE), "POINT(MIN_VALUE MIN_VALUE)");

    }

    /**
     * @see Geometries#lineStringFromWkt(String)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void errorLineStringWkt() {
        String wkt;
        wkt = "LINESTRING(0,0)";
        Geometries.lineStringFromWkt(wkt);

    }


}
