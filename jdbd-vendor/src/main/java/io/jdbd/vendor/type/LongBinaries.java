package io.jdbd.vendor.type;

import io.jdbd.type.LongBinary;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public abstract class LongBinaries implements LongBinary {

    public static LongBinary fromArray(byte[] array) {
        return new ArrayLongBinary(array);
    }

    /**
     * @param path should in {@code java.io.tmpdir} directory or sub directory.
     */
    public static LongBinary fromTempPath(Path path) {
        return new PathLongBinary(path);
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
            throw new IllegalStateException(String.format("Not support %s", FileChannel.class.getName()));
        }


    }

    private static final class PathLongBinary implements LongBinary {

        private final Path path;

        private PathLongBinary(Path path) {
            this.path = path;
            TempFiles.addTempPath(path);
        }

        @Override
        protected void finalize() throws Throwable {
            try {
                TempFiles.removeTempPath(this.path);
                Files.deleteIfExists(this.path);
            } catch (Throwable e) {
                //here don't need throw exception
            }
        }

        @Override
        public boolean isArray() {
            return false;
        }

        @Override
        public byte[] asArray() throws IllegalStateException {
            throw new IllegalStateException(String.format("Not support %s", byte[].class.getName()));
        }

        @Override
        public FileChannel openReadOnlyChannel() throws IOException {
            return FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);

        }


    }


}
