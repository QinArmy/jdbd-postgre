package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.Groups;
import io.jdbd.vendor.TestUtils;
import io.jdbd.vendor.util.BufferUtils;
import io.jdbd.vendor.util.JdbdDigestUtils;
import io.jdbd.vendor.util.JdbdTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @see Geometries
 */
@Test(groups = {Groups.GEOMETRIES})
public class GeometriesSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(GeometriesSuiteTests.class);

    private static final Queue<Path> PATH_QUEUE = new LinkedBlockingDeque<>();

    @BeforeClass
    public static void beforeClass() {
        Runtime.getRuntime().addShutdownHook(new Thread(GeometriesSuiteTests::deletePathQueue));
    }

    public static void deletePathQueue() {
        try {
            Path path;
            while ((path = PATH_QUEUE.poll()) != null) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


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
        wkbBytes = point.asWkb(true);
        result = Geometries.pointFromWkb(wkbBytes, 0);
        assertEquals(result, point, "result");

        wkbBytes = result.asWkb(false);
        result = Geometries.pointFromWkb(wkbBytes, 0);
        assertEquals(result, point, "result");


        x = Double.MAX_VALUE;
        y = Double.MIN_VALUE;

        point = Geometries.point(x, y);
        wkbBytes = point.asWkb(false);
        result = Geometries.pointFromWkb(wkbBytes, 0);
        assertEquals(result, point, "result");

        wkbBytes = result.asWkb(true);
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
        LineString lineString, result;
        List<Point> pointList;
        byte[] wkbBytes;

        pointList = new ArrayList<>(2);
        pointList.add(Geometries.point(0, 0));
        pointList.add(Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE));

        lineString = Geometries.lineString(pointList);

        wkbBytes = lineString.asWkb(false);
        LOG.info("wkt text:{}", lineString.asWkt());
        String wkb = BufferUtils.hexEscapesText(wkbBytes, wkbBytes.length);
        LOG.info("wkb text:{}", wkb);
        LOG.info("wkb length:{}", wkb.length());
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

        wkbBytes = result.asWkb(false);
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

        pointList = new ArrayList<>(4);
        pointList.add(Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE));
        pointList.add(Geometries.point(0, 0));
        pointList.add(Geometries.point(-1, 3));
        pointList.add(Geometries.point(-1, -1));

        lineString = Geometries.lineString(pointList);

        wkbBytes = lineString.asWkb(true);
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

        wkbBytes = result.asWkb(false);
        result = Geometries.lineStringFromWkb(wkbBytes, 0);
        assertEquals(result, lineString, "result");

    }

    /**
     * @see Geometries#lineStringFromWkt(String)
     */
    @Test
    public void lineStringFromWkt() {
        String wkt, resultWkt;
        LineString lineString, result;
        List<Point> pointList;

        wkt = String.format("LINESTRING(0 0,%s %s)", Double.MAX_VALUE, Double.MIN_VALUE);
        lineString = Geometries.lineStringFromWkt(wkt);
        resultWkt = lineString.asWkt();
        result = Geometries.lineStringFromWkt(resultWkt);
        assertEquals(result, lineString, "resultWkt");

        pointList = lineString.pointList();
        assertEquals(pointList.size(), 2, "pointList size");
        assertEquals(pointList.get(0), Geometries.point(0, 0), "zero point");
        assertEquals(pointList.get(1), Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE), "POINT(MIN_VALUE MIN_VALUE)");


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

    @Test(dependsOnMethods = {"lineStringFromWkb"})
    public void lineStringFromWkbPath() throws Exception {
        final int pointSize = 10;
        final Random random = new Random();

        List<Point> pointList = new ArrayList<>(pointSize);
        int textLength = pointSize - 1;
        Point point;

        for (int i = 0; i < pointSize; i++) {
            point = Geometries.point(random.nextDouble(), random.nextDouble());
            textLength += point.getPointTextLength();
            pointList.add(point);
        }
        final LineString lineString;
        lineString = MemoryLineString.unsafeLineString(pointList, textLength);

        LineString result;
        final Path originalPath, wkbPath;
        final String pathFix = LocalDate.now().format(JdbdTimeUtils.CLOSE_DATE_FORMATTER);
        originalPath = Files.createTempFile(pathFix, null);
        wkbPath = Files.createTempFile(pathFix, null);

        lineString.asWkbToPath(true, originalPath);
        result = Geometries.lineStringFromWkbPath(originalPath, 0L);
        result.asWkbToPath(true, wkbPath);

        assertTrue(JdbdDigestUtils.compareMd5(originalPath, wkbPath), "originalPath and wkbPath md5");

        result = Geometries.lineStringFromWkbPath(wkbPath, 0L);
        assertEquals(result, lineString, "result");

        result.asWkbToPath(false, wkbPath);

        result = Geometries.lineStringFromWkbPath(wkbPath, 0L);

        assertEquals(result, lineString, "result");

        Files.delete(originalPath);
        Files.delete(wkbPath);

    }

    @Test(dependsOnMethods = {"lineStringFromWkbPath"})
    public void largeLineStringFromWkbPath() throws IOException {
        final Path dir = Paths.get(TestUtils.getTargetTestClassesPath().toString(), "my-local/geometries");
        final Path original = Paths.get(dir.toString(), "largeLineString.wkb");
        final Path wkbPath = Paths.get(dir.toString(), "largeLineString_1.wkb");

        PATH_QUEUE.add(original);
        PATH_QUEUE.add(wkbPath);

        final boolean bigEndian = true;
        if (!Files.exists(original)) {
            createLargeLineStringPath(bigEndian, original);
        }
        LOG.info("original size:{}", Files.size(original));
        LineString lineString;
        lineString = PathLineString.fromWkbPath(original, 0L);

        lineString.asWkbToPath(bigEndian, wkbPath);

        assertEquals(Files.size(wkbPath), Files.size(original), "original and wkbPath size");
        LOG.info("start assert original and wkbPath MD5");
        assertTrue(JdbdDigestUtils.compareMd5(original, wkbPath), "original and wkbPath MD5");
        Files.deleteIfExists(wkbPath);

        LOG.info("create new wkbPath");
        lineString.asWkbToPath(!bigEndian, wkbPath);

        LineString result;
        LOG.info("read new wkbPath");
        result = PathLineString.fromWkbPath(wkbPath, 0L);
        result.asWkbToPath(bigEndian, wkbPath);

        assertEquals(Files.size(wkbPath), Files.size(original), "original and wkbPath size");
        LOG.info("start assert original and wkbPath MD5");
        assertTrue(JdbdDigestUtils.compareMd5(original, wkbPath), "original and wkbPath MD5");

        for (int i = 0; i < 1000000; i++) {
            PathLineString.fromWkbPath(wkbPath, 0L);
        }
    }

    @Test(dependsOnMethods = {"lineStringFromWkbPath"})
    public void pathLineStringFromWktPath() throws Exception {
        final Path dir = Paths.get(TestUtils.getTargetTestClassesPath().toString(), "my-local/geometries");
        final Path original = Paths.get(dir.toString(), "wktLargeLineString.wkt");
        final Path wkbPath = Paths.get(dir.toString(), "wktLargeLineString_1.wkt");

        PATH_QUEUE.add(original);
        PATH_QUEUE.add(wkbPath);

        // Thread.sleep(1000 * 1000L);


    }

    private void createLargeLineStringPath(final boolean bigEndian, final Path path) throws IOException {
        final long startTime = System.currentTimeMillis();
        if (!Files.exists(path)) {
            Path dir = path.getParent();
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            Files.createFile(path);
        }

        try (OutputStream out = Files.newOutputStream(path, StandardOpenOption.WRITE)) {

            final long pointSize = 44;
            out.write(Geometries.createWkbPrefix(bigEndian, LineString.WKB_TYPE_LINE_STRING, (int) pointSize));
            final Random random = new Random();
            final byte[] buffer = new byte[Geometries.get16BufferLength(pointSize << 4)];
            int length = 0, offset = 0;
            for (long i = 0; i < pointSize; i++) {
                if (length == 0) {
                    length = (int) Math.min(buffer.length >> 4, pointSize - i) << 4;
                    offset = 0;
                }
                Geometries.doPointAsWkb(random.nextDouble(), random.nextDouble(), bigEndian, buffer, offset);
                offset += 16;

                if (offset == length) {
                    out.write(buffer, 0, length);
                    length = 0;
                }

                if ((i & 0xFFFF_FFFL) == 0) {
                    LOG.info("create large LineString process {}%", (i / (double) pointSize) * 100);
                }

            }
            LOG.info("create large LineString process 100% ,cost {}ms", System.currentTimeMillis() - startTime);
        } catch (Throwable e) {
            if (Files.deleteIfExists(path)) {
                LOG.info("delete {}", path);
            }
            throw e;
        }


    }


}
