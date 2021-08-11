package io.jdbd.postgre.type;

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
import java.util.function.Consumer;

import static org.testng.Assert.*;

/**
 * @see PgGeometries
 */
public class PgGeometryUtilsUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgGeometryUtilsUnitTests.class);


    /**
     * @see PgGeometries#doReadPoints(String, int, Consumer)
     */
    @Test
    public void doReadPoints() {
        LOG.info("doReadPoints test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);
        int newIndex;
        final Consumer<Double> consumer = d -> out.writeLong(Double.doubleToLongBits(d));
        try {
            pointsText = " (0, 0 ) , (1, 1)     ";
            newIndex = PgGeometries.doReadPoints(pointsText, 0, consumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 2 * 16, "point count");
            out.clear();

            pointsText = String.format(" (0, 0 ) , (1, 1),(%s,%s)  ,(454.0,32.2)   ", Double.MAX_VALUE, Double.MIN_VALUE);
            newIndex = PgGeometries.doReadPoints(pointsText, 0, consumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 4 * 16, "point count");
        } finally {
            out.release();
        }

        LOG.info("doReadPoints test success.");
    }

    /**
     * @see PgGeometries#doReadPoints(String, int, Consumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError1() {
        LOG.info("doReadPointsWithError1 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,    ";
        final Consumer<Double> consumer = d -> out.writeLong(Double.doubleToLongBits(d));
        try {
            PgGeometries.doReadPoints(pointsText, 0, consumer);
            fail("doReadPointsWithError1 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError1 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#doReadPoints(String, int, Consumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError2() {
        LOG.info("doReadPointsWithError2 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ()    ";
        final Consumer<Double> consumer = d -> out.writeLong(Double.doubleToLongBits(d));
        try {
            PgGeometries.doReadPoints(pointsText, 0, consumer);
            fail("doReadPointsWithError2 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError2 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#doReadPoints(String, int, Consumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError3() {
        LOG.info("doReadPointsWithError3 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,(3434,    ";
        final Consumer<Double> consumer = d -> {
            out.writeLong(Double.doubleToLongBits(d));
        };
        try {
            PgGeometries.doReadPoints(pointsText, 0, consumer);
            fail("doReadPointsWithError3 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError3 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#lineSegmentToWkb(String, boolean)
     */
    @Test
    public void lineSegmentToWbk() {
        LOG.info("lineSegmentToWbk test start.");

        String pointsText;
        byte[] wkb;
        final boolean bigEndian = false;

        pointsText = "[ (0, 0 ) , (1, 1)]";
        wkb = PgGeometries.lineSegmentToWkb(pointsText, bigEndian);
        LOG.info("WKB type:{}", JdbdNumbers.readIntFromEndian(false, wkb, 1, 4));
        final String wkt = "LINESTRING(0 0,1 1)";

        assertTrue(Arrays.equals(wkb, GeometryUtils.lineStringToWkb(wkt, bigEndian)), "wkb error");
        LOG.info("lineSegmentToWbk test success.");
    }

    /**
     * @see PgGeometries#doReadPoints(String, int, Consumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void lineSegmentToWbkWithError1() {
        LOG.info("lineSegmentToWbkWithError1 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = "[ (0, 0 ) , (1, 1) this is evil. ]";

        try {
            PgGeometries.lineSegmentToWkb(pointsText, false);
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


}
