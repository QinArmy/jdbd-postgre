package io.jdbd.vendor.util;

import org.qinarmy.util.BufferWrapper;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @see Geometries
 */
@Test
public class GeometriesSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(GeometriesSuiteTests.class);

    /**
     * @see Geometries#readAndWritePoints(BufferWrapper, GenericGeometries.WkbOUtWrapper, WkbType)
     */
    @Test
    public void readAndWritePoints() {
        WkbType wkbType;
        String pointText;
        BufferWrapper inWrapper;
        GenericGeometries.WkbOUtWrapper outWrapper;
        int pointCount;


        wkbType = WkbType.LINE_STRING;
        pointText = String.format("0 0, 1.3 3.4 , %s %s ,0 0)", Double.MAX_VALUE, Double.MIN_VALUE);

        inWrapper = new BufferWrapper(pointText.getBytes(StandardCharsets.US_ASCII));
        outWrapper = new GenericGeometries.WkbOUtWrapper(1024, true);
        pointCount = Geometries.readAndWritePoints(inWrapper, outWrapper, wkbType);

        assertEquals(inWrapper.buffer.get(), ')', pointText);
        assertEquals(pointCount, 4, pointText);


        wkbType = WkbType.MULTI_POINT;
        pointText = String.format("  (   0 0),( 1.3 3.4 ), (%s %s)  )", Double.MAX_VALUE, Double.MIN_VALUE);

        inWrapper = new BufferWrapper(pointText.getBytes(StandardCharsets.US_ASCII));
        outWrapper = new GenericGeometries.WkbOUtWrapper(wkbType.coordinates() * 8 * 3, true);
        pointCount = Geometries.readAndWritePoints(inWrapper, outWrapper, wkbType);

        assertEquals(inWrapper.buffer.get(), ')', pointText);
        assertEquals(pointCount, 3, pointText);

    }

    /**
     * @see Geometries#readAndWriteLinearRing(BufferWrapper, GenericGeometries.WkbMemoryWrapper, WkbType)
     */
    @Test
    public void readAndWriteLinearRing() {
        WkbType wkbType;
        String linearRingText;
        BufferWrapper inWrapper;
        GenericGeometries.WkbMemoryWrapper outWrapper;
        int linearRingCount;

        wkbType = WkbType.POLYGON;
        linearRingText = " ( 0 0,1.3 3.4, 5.2 5.7, 0 0) )  ";

        inWrapper = new BufferWrapper(linearRingText.getBytes(StandardCharsets.US_ASCII));
        outWrapper = new GenericGeometries.WkbMemoryWrapper(1024, true);
        linearRingCount = Geometries.readAndWriteLinearRing(inWrapper, outWrapper, wkbType);

        assertEquals(inWrapper.buffer.get(), ')', linearRingText);
        assertEquals(linearRingCount, 1, linearRingText);


        wkbType = WkbType.POLYGON;
        linearRingText = String.format(" ( 0 0,1.3 3.4, 5.2 5.7, 0 0) , ( 0 0,1.3 3.4, %s %s, 0 0))  "
                , Double.MAX_VALUE, Double.MIN_VALUE);

        inWrapper = new BufferWrapper(linearRingText.getBytes(StandardCharsets.US_ASCII));
        outWrapper = new GenericGeometries.WkbMemoryWrapper(1024, true);
        linearRingCount = Geometries.readAndWriteLinearRing(inWrapper, outWrapper, wkbType);

        assertEquals(inWrapper.buffer.get(), ')', linearRingText);
        assertEquals(linearRingCount, 2, linearRingText);
    }

    /**
     * @see Geometries#pointToWkb(String, boolean)
     */
    @Test
    public void pointToWkb() {
        LOG.info("pointToWkb test start");
        String wktText;
        byte[] wkbArray;
        Pair<Double, Double> pair;

        wktText = " POINT EMPTY  ";
        wkbArray = Geometries.pointToWkb(wktText, true);
        assertEquals(Geometries.readElementCount(wkbArray), 0, wktText);
        LOG.info("'{}' WKT : {}", wktText, Geometries.pointToWkt(wkbArray));


        wktText = " POINT  ( 0 0)  ";
        wkbArray = Geometries.pointToWkb(wktText, true);
        pair = Geometries.readPointAsPair(wkbArray, 0);
        assertEquals(pair.getFirst(), Double.valueOf(0.0D), "x");
        assertEquals(pair.getSecond(), Double.valueOf(0.0D), "y");
        LOG.info("'{}' WKT : {}", wktText, Geometries.pointToWkt(wkbArray));


        wktText = String.format(" POINT  ( %s %s)  ", Double.MAX_VALUE, Double.MIN_VALUE);
        wkbArray = Geometries.pointToWkb(wktText, true);
        pair = Geometries.readPointAsPair(wkbArray, 0);
        assertEquals(pair.getFirst(), Double.valueOf(Double.MAX_VALUE), "x");
        assertEquals(pair.getSecond(), Double.valueOf(Double.MIN_VALUE), "y");
        LOG.info("'{}' WKT : {}", wktText, Geometries.pointToWkt(wkbArray));

        LOG.info("pointToWkb test success");
    }

    /**
     * @see Geometries#wkbEquals(byte[], byte[])
     */
    @Test(dependsOnMethods = "pointToWkb")
    public void geometryEquals() {
        LOG.info("pointEquals test start");
        String wktText;
        byte[] wkbArrayOne, wkbArrayTow;

        wktText = "POINT(1.0 2.3)";
        wkbArrayOne = Geometries.pointToWkb(wktText, true);
        wkbArrayTow = Geometries.pointToWkb(wktText, false);

        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        wktText = String.format("POINT(%s %s)", Double.MAX_VALUE, Double.MIN_VALUE);
        wkbArrayOne = Geometries.pointToWkb(wktText, true);
        wkbArrayTow = Geometries.pointToWkb(wktText, false);

        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        LOG.info("pointEquals test success");
    }

    /**
     * @see Geometries#lineStringToWkb(String, boolean)
     * @see Geometries#lineStringToWkt(byte[])
     */
    @Test
    public void lineStringToWkb() {
        LOG.info("lineStringToWkb test start");
        String wktText, wktTextTwo;
        byte[] wkbArrayOne, wkbArrayTow;

        wktText = " LINESTRING EMPTY ";
        wkbArrayOne = Geometries.lineStringToWkb(wktText, true);
        wkbArrayTow = Geometries.lineStringToWkb(wktText, false);
        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);
        wktTextTwo = Geometries.lineStringToWkt(wkbArrayOne);
        LOG.info("'{}' WKT : {}", wktText, wktTextTwo);

        wkbArrayTow = Geometries.lineStringToWkb(wktTextTwo, true);
        assertTrue(Geometries.wkbEquals(wkbArrayTow, wkbArrayOne), wktText);


        wktText = String.format(" LINESTRING (  0 0, 1.0 3.3 ,   %s %s  )", Double.MAX_VALUE, Double.MIN_VALUE);
        wkbArrayOne = Geometries.lineStringToWkb(wktText, true);
        wkbArrayTow = Geometries.lineStringToWkb(wktText, false);
        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);
        wktTextTwo = Geometries.lineStringToWkt(wkbArrayOne);
        LOG.info("'{}' WKT : {}", wktText, wktTextTwo);

        wkbArrayTow = Geometries.lineStringToWkb(wktTextTwo, true);
        assertTrue(Geometries.wkbEquals(wkbArrayTow, wkbArrayOne), wktText);

        LOG.info("lineStringToWkb test success");
    }

    /**
     * @see Geometries#polygonToWkb(String, boolean)
     * @see Geometries#polygonToWkt(byte[])
     */
    @Test
    public void polygonToWkb() {
        LOG.info("polygonToWkb test start");
        String wktText, wktTextTwo;
        byte[] wkbArrayOne, wkbArrayTow;

        wktText = "POLYGON EMPTY";
        wkbArrayOne = Geometries.polygonToWkb(wktText, true);
        wkbArrayTow = Geometries.polygonToWkb(wktText, false);
        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        wktTextTwo = Geometries.polygonToWkt(wkbArrayOne);
        LOG.info("POLYGON WKT compare:\n{}\n{}", wktText, wktTextTwo);
        wkbArrayTow = Geometries.polygonToWkb(wktTextTwo, true);
        assertTrue(Geometries.wkbEquals(wkbArrayTow, wkbArrayOne), wktTextTwo);


        wktText = "POLYGON((0 0,0 1,0 3,0 0))";
        wkbArrayOne = Geometries.polygonToWkb(wktText, true);
        wkbArrayTow = Geometries.polygonToWkb(wktText, false);
        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        wktTextTwo = Geometries.polygonToWkt(wkbArrayOne);
        LOG.info("POLYGON WKT compare:\n{}\n{}", wktText, wktTextTwo);
        wkbArrayTow = Geometries.polygonToWkb(wktTextTwo, true);
        assertTrue(Geometries.wkbEquals(wkbArrayTow, wkbArrayOne), wktTextTwo);

        wktText = String.format("POLYGON((0 0,0 1,0 3,0 0),(3 4,0 1,0 3,%s %s,3 4))"
                , Double.MAX_VALUE, Double.MIN_VALUE);
        wkbArrayOne = Geometries.polygonToWkb(wktText, true);
        wkbArrayTow = Geometries.polygonToWkb(wktText, false);
        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        wktTextTwo = Geometries.polygonToWkt(wkbArrayOne);
        LOG.info("POLYGON WKT compare:\n{}\n{}", wktText, wktTextTwo);
        wkbArrayTow = Geometries.polygonToWkb(wktTextTwo, true);
        assertTrue(Geometries.wkbEquals(wkbArrayTow, wkbArrayOne), wktTextTwo);

        LOG.info("polygonToWkb test success");
    }

    /**
     * @see Geometries#multiPointToWkb(String, boolean)
     */
    @Test
    public void multiPointToWkb() {
        //TODO test
        LOG.info("multiPointToWkb test start");
        String wktText;
        byte[] wkbArrayOne, wkbArrayTow;

        wktText = "MULTIPOINT((0 0),(0 1),(0 3),(0 0))";
        wkbArrayOne = Geometries.multiPointToWkb(wktText, true);
        wkbArrayTow = Geometries.multiPointToWkb(wktText, false);

        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        LOG.info("WKT one : {}", Geometries.multiPointToWkt(wkbArrayOne));
        LOG.info("WKT tow : {}", Geometries.multiPointToWkt(wkbArrayTow));


        LOG.info("multiPointToWkb test success");
    }

    @Test
    public void multiLineString() {
        LOG.info("multiLineString test start");
        String wktText;
        byte[] wkbArrayOne, wkbArrayTow;

        wktText = "MULTILINESTRING((0.0 1.3 ,3 3))";
        wkbArrayOne = Geometries.multiLineStringToWkb(wktText, true);
        wkbArrayTow = Geometries.multiLineStringToWkb(wktText, false);

        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        LOG.info("WKT one : {}", Geometries.multiLineStringToWkt(wkbArrayOne));
        LOG.info("WKT tow : {}", Geometries.multiLineStringToWkt(wkbArrayTow));

        LOG.info("multiLineString test success");
    }

    @Test
    public void multiPolygon() {
        LOG.info("multiPolygon test start");
        String wktText;
        byte[] wkbArrayOne, wkbArrayTow;

        wktText = "MULTILINESTRING((0.0 1.3 ,3 3))";
        wkbArrayOne = Geometries.multiPolygonToWkb(wktText, true);
        wkbArrayTow = Geometries.multiPolygonToWkb(wktText, false);

        assertTrue(Geometries.wkbEquals(wkbArrayOne, wkbArrayTow), wktText);

        LOG.info("WKT one : {}", Geometries.multiLineStringToWkt(wkbArrayOne));
        LOG.info("WKT tow : {}", Geometries.multiLineStringToWkt(wkbArrayTow));

        LOG.info("multiPolygon test success");
    }


}
