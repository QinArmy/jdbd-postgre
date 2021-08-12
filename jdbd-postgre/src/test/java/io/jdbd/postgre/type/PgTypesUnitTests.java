package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Line;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.type.Geometries;
import io.jdbd.vendor.util.GeometryUtils;
import io.jdbd.vendor.util.JdbdNumbers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static org.testng.Assert.*;

/**
 * @see PgTypes
 */
public class PgTypesUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgTypesUnitTests.class);


    /**
     * @see PgTypes#doReadPoints(String, int, BiConsumer)
     */
    @Test
    public void doReadPoints() {
        LOG.info("doReadPoints test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);
        int newIndex;
        final BiConsumer<Double, Double> pointConsumer = PgTypes.writePointWkbFunction(false, out);
        try {
            pointsText = " (0, 0 ) , (1, 1)     ";
            newIndex = PgTypes.doReadPoints(pointsText, 0, pointConsumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 2 * 16, "point count");
            out.clear();

            pointsText = String.format(" (0, 0 ) , (1, 1),(%s,%s)  ,(454.0,32.2)   ", Double.MAX_VALUE, Double.MIN_VALUE);
            newIndex = PgTypes.doReadPoints(pointsText, 0, pointConsumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 4 * 16, "point count");
        } finally {
            out.release();
        }

        LOG.info("doReadPoints test success.");
    }

    /**
     * @see PgTypes#doReadPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError1() {
        LOG.info("doReadPointsWithError1 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,    ";
        final BiConsumer<Double, Double> pointConsumer = PgTypes.writePointWkbFunction(false, out);
        try {
            PgTypes.doReadPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError1 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError1 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgTypes#doReadPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError2() {
        LOG.info("doReadPointsWithError2 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ()    ";
        final BiConsumer<Double, Double> pointConsumer = PgTypes.writePointWkbFunction(false, out);
        try {
            PgTypes.doReadPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError2 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError2 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgTypes#doReadPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError3() {
        LOG.info("doReadPointsWithError3 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,(3434,    ";
        final BiConsumer<Double, Double> pointConsumer = PgTypes.writePointWkbFunction(false, out);
        try {
            PgTypes.doReadPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError3 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError3 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgTypes#lineSegmentToWkb(String, boolean)
     */
    @Test
    public void lineSegmentToWbk() {
        LOG.info("lineSegmentToWbk test start.");

        String pointsText;
        byte[] wkb;
        final boolean bigEndian = false;

        pointsText = "[ (0, 0 ) , (1, 1)]";
        wkb = PgTypes.lineSegmentToWkb(pointsText, bigEndian);
        LOG.info("WKB type:{}", JdbdNumbers.readIntFromEndian(false, wkb, 1, 4));
        final String wkt = "LINESTRING(0 0,1 1)";

        assertTrue(Arrays.equals(wkb, GeometryUtils.lineStringToWkb(wkt, bigEndian)), "wkb error");
        LOG.info("lineSegmentToWbk test success.");
    }

    /**
     * @see PgTypes#doReadPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void lineSegmentToWbkWithError1() {
        LOG.info("lineSegmentToWbkWithError1 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = "[ (0, 0 ) , (1, 1) this is evil. ]";

        try {
            PgTypes.lineSegmentToWkb(pointsText, false);
            fail("lineSegmentToWbkWithError1 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("lineSegmentToWbkWithError1 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }


    /**
     * @see PgLine#parse(String)
     */
    @Test
    public void pgLineParse() {
        String text;
        PgLine line;
        double a, b, c;
        a = 0;
        b = 1.3;
        c = 5;
        text = String.format("{%s,%s,%s}", a, b, c);
        line = PgLine.parse(text);
        assertEquals(line.toString(), text, "PgLine.toString()");
        assertEquals(line, PgLine.create(a, b, c), "PgLine.equals()");

        a = Double.MIN_VALUE;
        b = Double.MAX_VALUE;
        c = 5;
        text = String.format("{%s,%s,%s}", a, b, c);
        line = PgLine.parse(text);
        assertEquals(line.toString(), text, "PgLine.toString()");
        assertEquals(line, PgLine.create(a, b, c), "PgLine.equals()");

    }

    /**
     * @see PgBox#parse(String)
     */
    @Test
    public void pgBoxParse() {
        final Point point1 = Geometries.point(1, 1.3), point2 = Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE);
        String text;
        PgBox box;
        text = String.format("(%s,%s),(%s,%s)", point1.getX(), point1.getY()
                , point2.getX(), point2.getY());

        box = PgBox.parse(text);

        assertEquals(box.getPoint1(), point1, "point1");
        assertEquals(box.getPoint2(), point2, "point2");
        assertEquals(box.toString(), text, "toString()");
        assertEquals(box, PgBox.create(point1, point2), "equals()");

    }

    @Test
    public void lineSegment() {
        final Point point1, point2;
        String text;
        Line line;

        point1 = Geometries.point(1.1, 3.3);
        point2 = Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE);

        text = String.format("[(%s,%s),(%s,%s)]", point1.getX(), point1.getY()
                , point2.getX(), point2.getY());
        line = PgTypes.lineSegment(text);

        assertEquals(line.getPoint1(), point1, "point1");
        assertEquals(line.getPoint2(), point2, "point2");

        text = String.format("LINESTRING(%s %s,%s %s)", point1.getX(), point1.getY()
                , point2.getX(), point2.getY());

        assertEquals(line.toString(), text, "wkt");

        boolean equals = GeometryUtils.wkbEquals(line.asArray(), GeometryUtils.lineStringToWkb(text, false));
        assertTrue(equals, "wkb");
    }


}
