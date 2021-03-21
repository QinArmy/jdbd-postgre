package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.util.JdbdDigestUtils;
import io.jdbd.vendor.util.JdbdNumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

final class DefaultPoint extends AbstractGeometry implements Point {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPoint.class);


    static final DefaultPoint ZERO = new DefaultPoint(0.0D, 0.0D);

    private final double x;

    private final double y;

    private final byte textLength;

    DefaultPoint(double x, double y) {
        this.x = x;
        this.y = y;
        this.textLength = (byte) (Double.toString(x).length() + Double.toString(y).length() + 1);
    }

    @Override
    public String asWkt() {
        return Geometries.pointAsWkt(this, new StringBuilder())
                .toString();
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.x, this.y);
    }

    @Override
    public boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Point) {
            Point p = (Point) obj;
            match = p.getX() == this.x && p.getY() == this.y;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public boolean deleteIfExists() {
        return false;
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    @Override
    public int elementCount() {
        return 1;
    }

    @Override
    public byte[] geometryMd5(final boolean bigEndian) {
        final byte[] pontBytes = new byte[16];
        if (bigEndian) {
            JdbdNumberUtils.longToBigEndian(Double.doubleToLongBits(this.x), pontBytes, 0, 8);
            JdbdNumberUtils.longToBigEndian(Double.doubleToLongBits(this.y), pontBytes, 8, 8);
        } else {
            JdbdNumberUtils.longToLittleEndian(Double.doubleToLongBits(this.x), pontBytes, 0, 8);
            JdbdNumberUtils.longToLittleEndian(Double.doubleToLongBits(this.y), pontBytes, 8, 8);
        }
        return JdbdDigestUtils.createMd5Digest().digest(pontBytes);
    }

    @Override
    public double getX() {
        return this.x;
    }

    @Override
    public double getY() {
        return this.y;
    }


    @Override
    public byte getPointTextLength() {
        return this.textLength;
    }

    @Override
    public long getTextLength() {
        return this.textLength;
    }

    @Override
    public long getWkbLength() {
        return Point.WKB_BYTES;
    }

    @Override
    public boolean isMemory() {
        // always true
        return true;
    }


    @Override
    public byte[] asWkb(final boolean bigEndian) {
        final byte[] wkbBytes = new byte[WKB_BYTES];
        Geometries.pointAsWkb(this, bigEndian, wkbBytes, 0);
        return wkbBytes;
    }

    @Override
    void writeAsWkb(boolean bigEndian, IoConsumer<byte[]> consumer) {
        // never here
        throw new UnsupportedOperationException();
    }

    @Override
    void writeAsWkt(IoConsumer<String> consumer) {
        // never here
        throw new UnsupportedOperationException();
    }


}
