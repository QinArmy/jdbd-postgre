package io.jdbd.vendor.type;

import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.type.geometry.WkbType;
import io.jdbd.vendor.util.GeometryUtils;
import reactor.core.publisher.Flux;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;

abstract class LineStrings {

    private LineStrings() {
        throw new UnsupportedOperationException();
    }

    static LineString fromWkbBytes(byte[] wkbArray) {
        GeometryUtils.checkLineStringWkb(wkbArray, WkbType.LINE_STRING);
        return new BytesLineString(wkbArray);
    }


    private static final class BytesLineString implements LineString {

        private final byte[] wkbArray;

        private BytesLineString(byte[] wkbArray) {
            this.wkbArray = wkbArray;
        }

        @Override
        public final boolean isArray() {
            return true;
        }

        @Override
        public final Flux<Point> points() {
            return GeometryUtils.lineStringToPoints(this.wkbArray);
        }

        @Override
        public final byte[] asArray() throws IllegalStateException {
            return Arrays.copyOf(this.wkbArray, this.wkbArray.length);
        }

        @Override
        public final FileChannel openReadOnlyChannel() throws IllegalStateException {
            throw LongBinaries.createNotSupportFileChannel();
        }

        @Override
        public final String toString() {
            return GeometryUtils.lineStringToWkt(this.wkbArray);
        }

    }


    private static final class PathLineString extends LongBinaries.PathLongBinary implements LineString {


        private PathLineString(Path path) {
            super(path);
        }

        @Override
        public final Flux<Point> points() {
            return GeometryUtils.lineStringToPoints(this.path);
        }


        @Override
        public final String toString() {
            //TODO
            return "";
        }


    }


}
