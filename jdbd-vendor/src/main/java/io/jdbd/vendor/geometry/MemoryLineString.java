package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Line;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.LinearRing;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.util.JdbdDigestUtils;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class MemoryLineString extends AbstractGeometry implements LineString {

    static LineString create(final List<Point> pointList) {
        final int size = pointList.size();
        if (size < 2 || size > MAX_POINT_LIST_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "pointList size[%s] not in [2,%s]", pointList.size(), MAX_POINT_LIST_SIZE));
        }
        final List<Point> newPointList = new ArrayList<>(size);
        return unsafeLineString(newPointList, obtainTextLength(pointList, newPointList));
    }

    static Line line(Point one, Point two) {
        if (one.equals(two)) {
            throw new IllegalArgumentException("Two point of Line can't be same point.");
        }
        List<Point> list = new ArrayList<>(2);
        list.add(one);
        list.add(two);
        int textLength = obtainTextLength(list, null);
        return new MemoryLine(list, textLength);
    }

    /**
     * inner api.
     */
    static LineString unsafeLineString(List<Point> pointList, int textLength) {
        final LineString lineString;
        final int size = pointList.size();
        if (size == 2) {
            lineString = new MemoryLineString(pointList, textLength);
        } else if (size > 3 && pointList.get(0).equals(pointList.get(size - 1))) {
            lineString = new MemoryLinearRing(pointList, textLength);
        } else {
            lineString = new MemoryLineString(pointList, textLength);
        }
        return lineString;
    }

    private static int obtainTextLength(List<Point> list, @Nullable List<Point> newList) {
        // each  point need a comma.
        int textLength = list.size() - 1;
        for (Point point : list) {
            if (textLength > 0) {
                textLength += point.getPointTextLength();
            }
            if (newList != null) {
                newList.add(point);
            }
        }
        return textLength;
    }

    private static final Logger LOG = LoggerFactory.getLogger(MemoryLineString.class);

    /**
     * length of {@code LINESTRING()} .
     */
    private static final byte WRAPPER_LENGTH = 12;

    private final List<Point> pointList;

    private final boolean small;

    private final int textLength;

    private MemoryLineString(final List<Point> pointList, final int textLength) {
        if (pointList.size() < 2) {
            throw new IllegalArgumentException("pointList error");
        }
        this.pointList = Collections.unmodifiableList(pointList);

        this.small = getWkbLength() < DefaultGeometryFactory.MAX_ARRAY_LENGTH
                && textLength > 0
                && (textLength + WRAPPER_LENGTH) < DefaultGeometryFactory.MAX_ARRAY_LENGTH;
        this.textLength = textLength;
    }

    @Override
    public int elementCount() {
        return this.pointList.size();
    }

    @Override
    public boolean isSmall() {
        return this.small;
    }

    @Override
    public long getTextLength() {
        return this.textLength;
    }

    @Override
    public long getWkbLength() {
        return (9L + pointList.size() * 16L);
    }

    @Override
    public final List<Point> pointList() {
        return this.pointList;
    }

    @Override
    public final Point startPoint() {
        return this.pointList.get(0);
    }

    @Override
    public final Point endPoint() {
        return this.pointList.get(this.pointList.size() - 1);
    }

    @Override
    public final boolean isClosed() {
        return this.pointList.size() > 3 && startPoint().equals(endPoint());
    }

    @Override
    public final boolean isLine() {
        return this.pointList.size() == 2;
    }

    @Override
    public final int hashCode() {
        return this.pointList.hashCode();
    }

    @Override
    public final boolean equals(Object obj) {
        boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof LineString) {
            LineString ls = (LineString) obj;
            if (ls.isSmall()) {
                match = this.pointList.equals(((LineString) obj).pointList());
            } else {
                try {
                    match = ls.elementCount() == elementCount()
                            && Arrays.equals(ls.geometryMd5(false), geometryMd5(false));
                } catch (IOException e) {
                    // occur io error, Underlying File maybe error, object Deprecated, not equals.
                    match = false;
                }
            }
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String asWkt() throws IllegalStateException {
        if (!isSmall()) {
            throw createCannotAsWktStringException();
        }
        return DefaultGeometryFactory.lineStringAsWkt(this, new StringBuilder(this.textLength + WRAPPER_LENGTH))
                .toString();
    }


    @Override
    public final byte[] asWkb(final boolean bigEndian) throws IllegalStateException {
        if (!isSmall()) {
            throw createCannotAsWkbArrayException();
        }
        final byte[] wkbBytes = new byte[(int) getWkbLength()];
        DefaultGeometryFactory.lineStringAsWkb(this.pointList, bigEndian, wkbBytes, 0);
        return wkbBytes;
    }


    final void writeAsWkb(final boolean bigEndian, final IoConsumer<byte[]> consumer) throws IOException {
        final List<Point> pointList = this.pointList;
        final int size = pointList.size();

        consumer.next(DefaultGeometryFactory.createWkbPrefix(bigEndian, LineString.WKB_TYPE_LINE_STRING, size));

        byte[] buffer = null;
        Point point;
        for (int i = 0, batch, offset = 0; i < size; i++) {
            if (buffer == null) {
                batch = Math.min(200, size - i);
                buffer = new byte[batch * 16];
                offset = 0;
            }
            point = pointList.get(i);
            DefaultGeometryFactory.doPointAsWkb(point.getX(), point.getY(), bigEndian, buffer, offset);
            offset += 16;

            if (offset == buffer.length) {
                consumer.next(buffer);
                buffer = null;
            }

        }


    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    @Override
    final void writeAsWkt(IoConsumer<String> consumer) throws IOException {
        final List<Point> pointList = this.pointList;
        final int size = pointList.size();

        int batch = Math.min(1000, size);

        StringBuilder builder = new StringBuilder(batch * 10)
                .append("LINESTRING(");
        Point point;
        for (int i = 0, offset = 0; i < size; i++) {
            if (builder == null) {
                batch = Math.min(1000, size - i);
                builder = new StringBuilder(batch * 10);
                offset = 0;
            }
            if (i > 0) {
                builder.append(",");
            }
            point = pointList.get(i);
            builder.append(point.getX())
                    .append(" ")
                    .append(point.getY());
            offset++;
            if (offset == batch) {
                if (i == size - 1) {
                    builder.append(")");
                }
                consumer.next(builder.toString());
                builder = null;
            }

        }

    }

    @Override
    public final byte[] geometryMd5(final boolean bigEndian) {
        final List<Point> pointList = this.pointList;
        final int size = pointList.size();
        final MessageDigest digest = JdbdDigestUtils.createMd5Digest();
        digest.update(DefaultGeometryFactory.createWkbPrefix(bigEndian, LineString.WKB_TYPE_LINE_STRING, size));
        final byte[] buffer = new byte[Math.min(size << 4, 2048)];
        Point point;
        for (int i = 0, offset = 0, length = 0; i < size; i++) {
            if (length == 0) {
                length = Math.min((size - i) << 4, buffer.length);
                offset = 0;
            }
            point = pointList.get(i);
            DefaultGeometryFactory.doPointAsWkb(point.getX(), point.getY(), bigEndian, buffer, offset);
            offset += 16;

            if (offset == length) {
                length = 0;
                digest.update(buffer, 0, length);
            }

        }
        return digest.digest();
    }

    @Override
    public final boolean deleteIfExists() {
        // always false.
        return false;
    }

    @Override
    public final Publisher<Point> pointStream() {
        return Flux.fromIterable(this.pointList);
    }

    /*################################## blow private method ##################################*/


    private static final class MemoryLine extends MemoryLineString implements Line {

        private static final Logger LOG = LoggerFactory.getLogger(MemoryLine.class);

        private MemoryLine(List<Point> pointList, final int textLength) {
            super(pointList, textLength);
            if (pointList.size() != 2) {
                throw new IllegalArgumentException("pointList size isn't 2.");
            }
        }

        @Override
        Logger obtainLogger() {
            return LOG;
        }
    }

    private static final class MemoryLinearRing extends MemoryLineString implements LinearRing {

        private static final Logger LOG = LoggerFactory.getLogger(MemoryLinearRing.class);

        private MemoryLinearRing(List<Point> pointList, final int textLength) {
            super(pointList, textLength);
            if (!isClosed()) {
                throw new IllegalArgumentException("pointList isn't closed.");
            }

        }

        @Override
        Logger obtainLogger() {
            return LOG;
        }

    }


}
