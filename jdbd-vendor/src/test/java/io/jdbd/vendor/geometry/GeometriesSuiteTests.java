package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

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


}
