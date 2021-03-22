package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Geometry;
import io.jdbd.vendor.util.JdbdTimeUtils;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

abstract class AbstractGeometry implements Geometry {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractGeometry.class);

    static final ConcurrentMap<Path, Boolean> PATH_MAP = new ConcurrentHashMap<>();


    private static void deleteFileOnExit() {
        try {
            final boolean debugEnabled = LOG.isDebugEnabled();
            for (Path path : PATH_MAP.keySet()) {
                if (Files.deleteIfExists(path) && debugEnabled) {
                    LOG.debug("delete {} success on Shutdown Hook", path);
                }
            }
            LOG.debug("delete on Shutdown Hook");
        } catch (IOException e) {
            throw new RuntimeException("delete temp file failure on Shutdown Hook.", e);
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(AbstractGeometry::deleteFileOnExit));
    }

    @Override
    public final Publisher<byte[]> asWkbStream(boolean bigEndian) {
        final Publisher<byte[]> publisher;
        if (isSmall()) {
            publisher = Mono.just(asWkb(bigEndian));
        } else {
            publisher = Flux.create(sink -> {
                try {
                    writeAsWkb(bigEndian, sink::next);
                    sink.complete();
                } catch (Throwable e) {
                    sink.error(e);
                }
            });
        }
        return publisher;
    }

    @Override
    public final long elementCountAsLong() {
        return Integer.toUnsignedLong(elementCount());
    }

    @Override
    public final Publisher<String> asWktStream() {
        final Publisher<String> publisher;
        if (isSmall()) {
            publisher = Mono.just(asWkt());
        } else {
            publisher = Flux.create(sink -> {
                try {
                    writeAsWkt(sink::next);
                    sink.complete();
                } catch (Throwable e) {
                    sink.error(e);
                }

            });
        }
        return publisher;
    }

    @Override
    public final void asWkbToPath(final boolean bigEndian, final Path path) throws IOException {
        if (!isValid()) {
            throw new IOException(String.format("Underlying file of %s instance have deleted.", getClass().getName()));
        }
        final boolean fileExists;
        fileExists = Files.exists(path, LinkOption.NOFOLLOW_LINKS);
        if (fileExists && !Files.isWritable(path)) {
            throw new IOException(String.format("File[%s] isn't writable.", path));
        }

        try {
            if (!isSmall() && copyWkbIfEndianMatch(bigEndian, path)) {
                return;
            }
            try (FileChannel out = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                asWkbToChannel(bigEndian, out);
            }
        } catch (Throwable e) {
            Logger logger = obtainLogger();
            if (!fileExists && Files.deleteIfExists(path) && logger.isDebugEnabled()) {
                logger.debug("delete created file[{}] after error occur.", path);
            }
            if (e instanceof Error) {
                throw (Error) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e.getMessage(), e);
            }
        }

    }

    @Override
    public final long asWkbToChannel(boolean bigEndian, FileChannel out) throws IOException {
        if (!out.isOpen()) {
            throw new IOException("FileChannel have closed.");
        }
        final long originalPosition;
        originalPosition = out.position();
        try {
            if (isSmall()) {
                out.write(ByteBuffer.wrap(asWkb(bigEndian)));
            } else {
                writeAsWkb(bigEndian, bytes -> out.write(ByteBuffer.wrap(bytes)));
            }
            return out.position() - originalPosition;
        } catch (Throwable e) {
            truncateOnError(e, out, originalPosition);
            if (e instanceof Error) {
                throw (Error) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e.getMessage(), e);
            }
        }


    }


    @Override
    public final void asWktToPath(Path path) throws IOException {
        if (!isValid()) {
            throw new IOException(String.format("Underlying file of %s instance have deleted.", getClass().getName()));
        }
        final boolean fileExists;
        fileExists = Files.exists(path, LinkOption.NOFOLLOW_LINKS);
        if (fileExists && !Files.isWritable(path)) {
            throw new IOException(String.format("File[%s] isn't writable.", path));
        }

        try {
            try (FileChannel out = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                asWktToChannel(out);
            }
        } catch (Throwable e) {
            Logger logger = obtainLogger();
            if (!fileExists && Files.deleteIfExists(path) && logger.isDebugEnabled()) {
                logger.debug("delete created file[{}] after error occur.", path);
            }
            if (e instanceof Error) {
                throw (Error) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e.getMessage(), e);
            }
        }

    }

    @Override
    public final long asWktToChannel(FileChannel out) throws IOException {
        if (!out.isOpen()) {
            throw new IOException("FileChannel have closed.");
        }
        final long originalPosition;
        originalPosition = out.position();
        try {
            if (isSmall()) {
                out.write(ByteBuffer.wrap(asWkt().getBytes()));
            } else {
                writeAsWkt(wkt -> out.write(ByteBuffer.wrap(wkt.getBytes())));
            }
            return out.position() - originalPosition;
        } catch (Throwable e) {
            truncateOnError(e, out, originalPosition);
            if (e instanceof Error) {
                throw (Error) e;
            } else if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e.getMessage(), e);
            }
        }

    }

    @Override
    public final String toString() {
        return isSmall() ? asWkt() : super.toString();
    }

    @Override
    public boolean deleteIfExists() throws IOException {
        if (isSmall()) {
            return false;
        }
        throw new UnsupportedOperationException();
    }

    /**
     * @see #asWkbStream(boolean)
     * @see #asWkbToPath(boolean, Path)
     */
    abstract void writeAsWkb(final boolean bigEndian, final IoConsumer<byte[]> consumer) throws IOException;

    /**
     * @see #asWktStream()
     * @see #asWktToPath(Path)
     */
    abstract void writeAsWkt(final IoConsumer<String> consumer) throws IOException;

    abstract Logger obtainLogger();

    /**
     * @see #asWkbToPath(boolean, Path)
     */
    boolean copyWkbIfEndianMatch(boolean bigEndian, Path target) throws IOException {
        return false;
    }


    @Override
    public boolean isValid() {
        if (isSmall()) {
            return true;
        }
        throw new UnsupportedOperationException();
    }



    /*################################## blow private method ##################################*/

    private void truncateOnError(Throwable e, final FileChannel out, final long originalPosition)
            throws IOException {
        if (!(e instanceof NonWritableChannelException)) {
            final long position;
            position = out.position();

            out.truncate(originalPosition);
            out.position(originalPosition);

            Logger logger = obtainLogger();
            if (logger.isDebugEnabled()) {
                logger.debug("Truncate truncate size[{}] after error occur.", position - originalPosition);
            }
        }

    }

    protected final IOException createUnderlyingFileModified(Path path) {
        return new IOException(String.format("%s underlying file[%s] is modified.", getClass().getName(), path));
    }

    static IllegalStateException createCannotAsWkbArrayException() {
        return new IllegalStateException("Geometry too large,can't as WKB with byte[].");
    }

    static IllegalStateException createCannotAsWktStringException() {
        return new IllegalStateException("Geometry too large,can't as WKT with String.");
    }

    static Path getTempDirectory() throws IOException {
        Path dir = Paths.get(System.getProperty("java.io.tmpdir"), "jdbdverdor"
                , LocalDate.now().format(JdbdTimeUtils.CLOSE_DATE_FORMATTER));
        if (Files.notExists(dir)) {
            Files.createDirectories(dir);
        }
        return dir;
    }

    static Path createWkbTempFile(String prefix) throws IOException {
        return Files.createTempFile(getTempDirectory(), prefix, ".wkb");
    }


}
