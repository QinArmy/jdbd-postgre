package io.jdbd.vendor.type;

import io.jdbd.type.LongBinary;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;

public abstract class LongBinaries implements LongBinary {

    public static LongBinary fromArray(byte[] array) {
        return new ArrayLongBinary(Objects.requireNonNull(array, "array"));
    }

    /**
     * @param path should in {@code java.io.tmpdir} directory or sub directory.
     */
    public static LongBinary fromTempPath(Path path) {
        return new PathLongBinary(Objects.requireNonNull(path, "path"));
    }

    static IllegalStateException creteNotSupportArrayError() {
        return new IllegalStateException("Non-underlying byte array,use openReadOnlyChannel() method.");
    }

    static IllegalStateException createNotSupportFileChannel() {
        return new IllegalStateException("Non-underlying file,use asArray() method.");
    }


    private LongBinaries() {

    }

    private static final class ArrayLongBinary implements LongBinary {

        private final byte[] array;

        private ArrayLongBinary(byte[] array) {
            this.array = array;
        }

        @Override
        public boolean isArray() {
            return true;
        }


        @Override
        public byte[] asArray() throws IllegalStateException {
            return Arrays.copyOf(this.array, this.array.length);
        }

        @Override
        public FileChannel openReadOnlyChannel() {
            throw createNotSupportFileChannel();
        }


    }

    static class PathLongBinary implements LongBinary {

        final Path path;

        protected PathLongBinary(Path path) {
            this.path = path;
            TempFiles.addTempPath(path);
        }

        @Override
        protected final void finalize() throws Throwable {
            try {
                TempFiles.removeTempPath(this.path);
                Files.deleteIfExists(this.path);
            } catch (Throwable e) {
                //here don't need throw exception
            }
        }

        @Override
        public final boolean isArray() {
            return false;
        }

        @Override
        public final byte[] asArray() throws IllegalStateException {
            throw creteNotSupportArrayError();
        }

        @Override
        public final FileChannel openReadOnlyChannel() throws IOException {
            return FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);

        }


    }


}
