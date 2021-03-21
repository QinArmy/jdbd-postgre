package io.jdbd.vendor.geometry;

import io.jdbd.type.geometry.Geometry;
import io.jdbd.vendor.util.JdbdTimeUtils;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
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
        if (isMemory()) {
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
        if (isMemory()) {
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
        try {
            if (!isMemory() && copyWkbIfEndianMatch(bigEndian, path)) {
                return;
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }

        try (OutputStream out = Files.newOutputStream(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            if (isMemory()) {
                out.write(asWkb(bigEndian));
            } else {
                writeAsWkb(bigEndian, out::write);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }

    }

    @Override
    public final void asWktToPath(Path path) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.WRITE)) {
            if (isMemory()) {
                writer.write(asWkt());
            } else {
                writeAsWkt(writer::write);
            }
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e.getMessage(), e);
        }

    }

    @Override
    public final String toString() {
        return isMemory() ? asWkt() : super.toString();
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


    protected final IOException createUnderlyingFileModified(Path path) {
        return new IOException(String.format("%s underlying file[%s] is modified.", getClass().getName(), path));
    }

    /**
     * @param tempPath if error ,delete tempPath.
     */
    static void copyFileFromStream(final Path tempPath, final long needBytes, final InputStream in
            , final MessageDigest digest) throws IOException {

        try (OutputStream out = Files.newOutputStream(tempPath, StandardOpenOption.WRITE)) {
            final byte[] buffer = new byte[2048];
            int readMaxLength = (int) Math.min(buffer.length, needBytes);
            int length;
            long readBytes = 0L;

            while ((length = in.read(buffer, 0, readMaxLength)) > 0) {
                digest.update(buffer, 0, length);
                out.write(buffer, 0, length);
                readBytes += length;
                if (readBytes == needBytes) {
                    break;
                }
                readMaxLength = (int) Math.min(buffer.length, needBytes - readBytes);
            }
            if (readBytes != needBytes) {
                throw createReadByteTooLessError(readBytes, needBytes);
            }
        } catch (Throwable e) {
            Files.deleteIfExists(tempPath);
            throw e;
        }

    }

    private static IllegalArgumentException createReadByteTooLessError(long readBytes, long needBytes) {
        return new IllegalArgumentException(
                String.format("Read bytes[%s] less than need bytes[%s]", readBytes, needBytes));
    }


}
