package io.jdbd.postgre.type;

import io.jdbd.type.Point;
import io.jdbd.type.geometry.Circle;
import io.jdbd.vendor.type.Geometries;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.function.Consumer;

import static org.testng.Assert.*;

/**
 * @see PgGeometries
 */
public class PgGeometriesUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgGeometriesUnitTests.class);

    /**
     * @see PgGeometries#point(String)
     */
    @Test
    public void point() {
        String text;
        Point point;

        text = "(0,0)";
        point = PgGeometries.point(text);
        assertEquals(point.getX(), 0, "x");
        assertEquals(point.getY(), 0, "y");

        text = "( 0,  3.3    )";
        point = PgGeometries.point(text);
        assertEquals(point.getX(), 0, "x");
        assertEquals(point.getY(), 3.3, "y");

        text = String.format("( %s,  %s    )", Double.MAX_VALUE, Double.MIN_VALUE);
        point = PgGeometries.point(text);
        assertEquals(point.getX(), Double.MAX_VALUE, "x");
        assertEquals(point.getY(), Double.MIN_VALUE, "y");
    }

    /**
     * @see PgGeometries#point(String)
     */
    @Test
    public void pointError() {
        String text;

        text = "0,9";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

        text = "(0,9";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

        text = "0,9)";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

        text = "(0 9)";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

    }

    /**
     * @see PgGeometries#readPoints(String, int, Consumer)
     */
    @Test
    public void doReadPoints() {
        LOG.info("doReadPoints test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);
        int newIndex;
        final Consumer<Point> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            pointsText = " (0, 0 ) , (1, 1)     ";
            newIndex = PgGeometries.readPoints(pointsText, 0, pointConsumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 2 * 16, "point count");
            out.clear();

            pointsText = String.format(" (0, 0 ) , (1, 1),(%s,%s)  ,(454.0,32.2)   ", Double.MAX_VALUE, Double.MIN_VALUE);
            newIndex = PgGeometries.readPoints(pointsText, 0, pointConsumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 4 * 16, "point count");
        } finally {
            out.release();
        }

        LOG.info("doReadPoints test success.");
    }

    /**
     * @see PgGeometries#readPoints(String, int, Consumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError1() {
        LOG.info("doReadPointsWithError1 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,    ";
        final Consumer<Point> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            PgGeometries.readPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError1 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError1 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#readPoints(String, int, Consumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError2() {
        LOG.info("doReadPointsWithError2 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ()    ";
        final Consumer<Point> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            PgGeometries.readPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError2 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError2 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#readPoints(String, int, Consumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError3() {
        LOG.info("doReadPointsWithError3 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,(3434,    ";
        final Consumer<Point> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            PgGeometries.readPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError3 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError3 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }



    @Test
    public void circle() {
        String text;
        Point p;
        double r;
        p = Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE);
        r = 10;
        text = String.format("<(%s,%s),%s>", p.getX(), p.getY(), r);

        Circle c = PgGeometries.circle(text);
        assertEquals(c.getCenter(), p, "center");
        assertEquals(c.getRadius(), r, "radius");
    }


}
