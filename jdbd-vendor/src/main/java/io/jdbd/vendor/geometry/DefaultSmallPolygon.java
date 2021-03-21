package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Geometry;
import io.jdbd.type.geometry.LinearRing;
import io.jdbd.type.geometry.Polygon;
import org.reactivestreams.Publisher;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

final class DefaultSmallPolygon implements Polygon, Geometry {


    static Polygon create(final Collection<LinearRing> linearRings) {
        if (linearRings.isEmpty()) {
            throw new IllegalArgumentException("linearRings must not empty.");
        }
        List<LinearRing> list = new ArrayList<>(linearRings.size());
        int textLength = linearRings.size() * 3, wkbLength = 9;
        for (LinearRing linearRing : linearRings) {
            textLength += linearRing.getTextLength();
            wkbLength += linearRing.getWkbLength();
            list.add(linearRing);
        }
        if (textLength < 0 || textLength > (1 << 30) - 9) {
            throw new IllegalArgumentException(String.format(
                    "WKT java.lang.String cannot express this POLYGON,please use %s."
                    , Polygon.class.getName()));
        }
        if (wkbLength < 0 || wkbLength > (1 << 30)) {
            throw new IllegalArgumentException(String.format("WKB byte[] cannot express this POLYGON,please use %s."
                    , Polygon.class.getName()));
        }
        return new DefaultSmallPolygon(list, textLength, wkbLength);
    }


    private final Collection<LinearRing> linearRings;

    private final int textLength;

    private final int wkbLength;

    private DefaultSmallPolygon(Collection<LinearRing> linearRings, int textLength, int wkbLength) {
        this.linearRings = Collections.unmodifiableCollection(linearRings);
        this.textLength = textLength;
        this.wkbLength = wkbLength;
    }

    @Override
    public int elementCount() {
        return 0;
    }

    @Override
    public byte[] geometryMd5(boolean bigEndian) throws IOException {
        return new byte[0];
    }

    @Override
    public boolean deleteIfExists() throws IOException {
        return false;
    }

    @Override
    public byte[] asWkb(boolean bigEndian) {
        return new byte[0];
    }

    @Override
    public String asWkt() {
        return null;
    }

    @Override
    public long getTextLength() {
        return this.textLength;
    }

    @Override
    public long getWkbLength() {
        return this.wkbLength;
    }

    @Override
    public Collection<LinearRing> linearRings() {
        return this.linearRings;
    }

    @Override
    public boolean isMemory() {
        return false;
    }

    @Override
    public Publisher<byte[]> asWkbStream(boolean bigEndian) {
        return null;
    }

    @Override
    public Publisher<String> asWktStream() {
        return null;
    }

    @Override
    public void asWkbToPath(boolean bigEndian, Path path) throws IOException {

    }

    @Override
    public void asWktToPath(Path path) throws IOException {

    }
}
