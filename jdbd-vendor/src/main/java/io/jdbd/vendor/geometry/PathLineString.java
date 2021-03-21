package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.util.JdbdDigestUtils;
import io.jdbd.vendor.util.JdbdStreamUtils;
import org.qinarmy.util.Pair;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

class PathLineString extends AbstractGeometry implements LineString {

    private static final Logger LOG = LoggerFactory.getLogger(PathLineString.class);


    static LineString fromWkbPath(final Path path, final long offset) throws IOException {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            // 1. handle hasBytes.
            final long hasBytes;
            hasBytes = Geometries.handleOffset(channel, offset);
            // record start position.
            final long startPosition = channel.position();
            // 2. read LineString prefix(byteOrder,WKB-TYPE,pointCount)
            final byte[] wkbArray = new byte[9];
            int length;
            if ((length = channel.read(ByteBuffer.wrap(wkbArray))) < wkbArray.length) {
                throw Geometries.createWkbFormatError(length, wkbArray.length);
            }
            final Pair<Boolean, Integer> pair;
            pair = Geometries.readWkbHead(wkbArray, 0, LineString.WKB_TYPE_LINE_STRING);
            if (pair.getSecond() == 0 || pair.getSecond() == 1) {
                throw Geometries.createWkbFormatError(9 + (pair.getSecond() << 4), 9 + (2 << 4));
            }

            final long pointsNeedBytes = (pair.getSecond() & Geometries.MAX_UNSIGNED_INT) << 4;
            final long needBytes = 9L + pointsNeedBytes;
            if (hasBytes < needBytes) {
                throw Geometries.createWkbFormatError(hasBytes, needBytes);
            }
            // 3. read start point and end point.
            final byte[] pointBufferArray = new byte[16];
            final ByteBuffer pointBuffer = ByteBuffer.wrap(pointBufferArray);
            if (channel.read(pointBuffer) != pointBufferArray.length) {
                throw Geometries.createWkbFormatError(hasBytes, needBytes);
            }
            final Point startPoint, endPoint;
            startPoint = Geometries.doPointFromWkb(pointBufferArray, pair.getFirst(), 0);
            // clear buffer for endPoint
            pointBuffer.clear();
            // position to last point start index
            channel.position(startPosition + needBytes - 16L);
            if (channel.read(pointBuffer) != pointBufferArray.length) {
                throw Geometries.createWkbFormatError(hasBytes, pointsNeedBytes);
            }
            endPoint = Geometries.doPointFromWkb(pointBufferArray, pair.getFirst(), 0);
            // 4. copy WKB to temp file and get MD5.
            final Path lineStringPath;
            lineStringPath = Files.createTempFile(getTempDirectory(), "linestring", ".wkb");
            // reset position
            channel.position(startPosition);
            final byte[] fileMd5;
            fileMd5 = JdbdStreamUtils.copyFromChannelWithMd5(lineStringPath, needBytes, channel, true);
            // 5. create instance.
            return new PathLineString(lineStringPath, fileMd5, pair, startPoint, endPoint);
        } catch (IllegalArgumentException | IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }

    }

    static LineString fromWktPath(final Path path, final long offset) throws IOException {
        try (FileChannel in = FileChannel.open(path)) {
            final long hasBytes;
            hasBytes = Geometries.handleOffset(in, offset);
            final byte[] bufferArray = new byte[(int) Math.min(hasBytes, 2048)];
            final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);

            final Charset charset = JdbdStreamUtils.fileEncodingOrUtf8();
            CharBuffer charBuffer;
            if (in.read(buffer) < 1) {
                throw Geometries.createWktFormatError(null, "");
            }
            buffer.flip();
            charBuffer = charset.decode(buffer);
            buffer.clear();

            for (int i = 0; in.read(buffer) > 0; i++) {
                charBuffer = charset.decode(buffer);
            }
            return null;
        } catch (IllegalArgumentException | IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }


    private final Path path;

    private final boolean bigEndian;

    private final byte[] fileMd5;

    /**
     * a unsigned int .
     */
    private final int pointCount;

    private final Point startPoint;

    private final Point endPoint;

    private PathLineString(Path path, byte[] fileMd5, Pair<Boolean, Integer> pair, Point startPoint, Point endPoint) {
        this.path = path;
        this.fileMd5 = fileMd5;
        this.bigEndian = pair.getFirst();
        this.pointCount = pair.getSecond();

        this.startPoint = startPoint;
        this.endPoint = endPoint;
        PATH_MAP.put(path, Boolean.TRUE);
    }

    @Override
    protected final void finalize() throws Throwable {
        try {
            PATH_MAP.remove(this.path);
            if (Files.deleteIfExists(this.path) && LOG.isDebugEnabled()) {
                LOG.debug("delete {}[{}] success.", PathLineString.class.getName(), this.path);
            }
        } catch (Throwable e) {
            // here don't need  throw error
            LOG.error("delete {}[{}] failure.", PathLineString.class.getName(), this.path, e);
        }

    }

    @Override
    public int hashCode() {
        return Objects.hash(this.pointCount, this.fileMd5);
    }

    @Deprecated
    @Override
    public boolean equals(Object obj) {
        boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof LineString) {
            LineString ls = (LineString) obj;
            try {
                match = ls.elementCount() == elementCount()
                        && Arrays.equals(ls.geometryMd5(this.bigEndian), this.fileMd5);
            } catch (IOException e) {
                // occur io error, Underlying File maybe error, object Deprecated, not equals.
                match = false;
            }
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final byte[] geometryMd5(final boolean bigEndian) throws IOException {
        byte[] md5Bytes;
        if (this.bigEndian == bigEndian) {
            md5Bytes = Arrays.copyOf(this.fileMd5, this.fileMd5.length);
        } else {
            md5Bytes = JdbdDigestUtils.fileMd5(this.path);
        }
        return md5Bytes;
    }

    @Override
    public final int elementCount() {
        return this.pointCount;
    }

    @Override
    public final byte[] asWkb(boolean bigEndian) throws IllegalStateException {
        throw new IllegalStateException(String.format("%s not in memory,please use asWkbStream(boolean) method."
                , LineString.class.getName()));
    }

    @Override
    public final String asWkt() throws IllegalStateException {
        throw new IllegalStateException(String.format("%s not in memory,please use asWktStream() method."
                , LineString.class.getName()));
    }

    @Override
    public final boolean isMemory() {
        // always false.
        return false;
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }

    @Override
    public final long getTextLength() {
        return Integer.MAX_VALUE;
    }

    @Override
    public final long getWkbLength() {
        return Integer.MAX_VALUE;
    }

    @Override
    public final List<Point> pointList() throws IllegalStateException {
        throw new IllegalStateException(String.format("%s not in memory,please use pointStream() method."
                , LineString.class.getName()));
    }

    @Override
    public final Publisher<Point> pointStream() {
        return Flux.create(sink -> {

            try (FileChannel in = FileChannel.open(this.path, StandardOpenOption.READ)) {

                final MessageDigest digest = JdbdDigestUtils.createMd5Digest();
                // 1. validate prefix.
                if (validateFilePrefix(in, digest)) {
                    sink.error(createUnderlyingFileModified(this.path));
                    return;
                }
                // 2. read points
                Geometries.readLineStringPoints(in, this.bigEndian, this.pointCount, sink::next, digest);

                // 3. validate md5 of underlying fil.
                if (Arrays.equals(digest.digest(), this.fileMd5)) {
                    sink.complete();
                } else {
                    sink.error(createUnderlyingFileModified(this.path));
                }
            } catch (IOException e) {
                sink.error(e);
            } catch (Throwable e) {
                sink.error(new IOException(e.getMessage(), e));
            }

        });
    }


    @Override
    public final Point startPoint() {
        return this.startPoint;
    }

    @Override
    public final Point endPoint() {
        return this.endPoint;
    }

    @Override
    public final boolean isClosed() {
        return (this.pointCount > 3 || this.pointCount < 0)
                && this.startPoint.equals(this.endPoint);
    }

    @Override
    public final boolean isLine() {
        return this.pointCount == 2;
    }

    @Override
    final boolean copyWkbIfEndianMatch(boolean bigEndian, Path target) throws IOException {
        boolean match = bigEndian == this.bigEndian;
        if (match) {
            Files.copy(this.path, target, StandardCopyOption.REPLACE_EXISTING);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Copy {} to {} success.", this.path, target);
            }
        }
        return match;
    }

    @Override
    final void writeAsWkb(final boolean bigEndian, final IoConsumer<byte[]> consumer) throws IOException {
        final byte[] md5Bytes;
        try (FileChannel in = FileChannel.open(this.path, StandardOpenOption.READ)) {

            final MessageDigest digest = JdbdDigestUtils.createMd5Digest();
            // 1.validate prefix.
            if (validateFilePrefix(in, digest)) {
                throw createUnderlyingFileModified(this.path);
            }
            // 2. write prefix
            consumer.next(Geometries.createWkbPrefix(bigEndian, LineString.WKB_TYPE_LINE_STRING
                    , this.pointCount));

            final IoConsumer<byte[]> wrapper;
            if (bigEndian == this.bigEndian) {
                wrapper = consumer;
            } else {
                wrapper = consumer.after(Geometries::notPointEndian);
            }
            // 3. write point data.
            Geometries.readPointsWkbAndConsumer(in, this.pointCount, wrapper, digest, true);
            md5Bytes = digest.digest();
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
        if (!Arrays.equals(md5Bytes, this.fileMd5)) {
            throw createUnderlyingFileModified(this.path);
        }
    }


    @Override
    final void writeAsWkt(final IoConsumer<String> consumer) throws IOException {
        try (FileChannel in = FileChannel.open(this.path, StandardOpenOption.READ)) {

            final MessageDigest digest = JdbdDigestUtils.createMd5Digest();
            // 1.validate prefix.
            if (validateFilePrefix(in, digest)) {
                throw createUnderlyingFileModified(this.path);
            }
            // 2. write prefix
            consumer.next("LINESTRING(");
            final boolean[] comma = new boolean[]{false};
            final IoConsumer<byte[]> wkbConsumer = wkb -> {
                StringBuilder builder = new StringBuilder((wkb.length >> 3) * 10);
                Geometries.doPointsWkbToWkt(wkb, this.bigEndian, builder, comma[0]);
                if (!comma[0]) {
                    comma[0] = true;
                }
                consumer.next(builder.toString());
            };
            // 3. write point data.
            Geometries.readPointsWkbAndConsumer(in, this.pointCount, wkbConsumer, digest, false);
            consumer.next(")");
            if (!Arrays.equals(digest.digest(), this.fileMd5)) {
                throw createUnderlyingFileModified(this.path);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public final boolean deleteIfExists() throws IOException {
        try {
            PATH_MAP.remove(this.path);
            return Files.deleteIfExists(this.path);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    /*################################## blow private method ##################################*/


    /**
     * @return true : modified.
     */
    private boolean validateFilePrefix(FileChannel in, MessageDigest digest) throws IOException {
        final byte[] bufferArray = new byte[9];
        final ByteBuffer buffer = ByteBuffer.wrap(bufferArray);
        if (in.read(buffer) != bufferArray.length) {
            return true;
        }
        final Pair<Boolean, Integer> pair;
        pair = Geometries.readWkbHead(bufferArray, 0, LineString.WKB_TYPE_LINE_STRING);
        if (pair.getFirst() != this.bigEndian || pair.getSecond() != this.pointCount) {
            return true;
        }
        digest.update(bufferArray);
        return false;
    }


}
