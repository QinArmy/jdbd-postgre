package io.jdbd.vendor.type;

import io.jdbd.type.LongString;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class LongStrings implements LongString {


    public static LongString fromString(String text) {
        return new ArrayLongString(text);
    }

    /**
     * @param path should in {@code java.io.tmpdir} directory or sub directory.
     */
    public static LongString fromTempPath(Path path) {
        return new PathLongString(path);
    }

    private LongStrings() {
    }


    private static final class ArrayLongString implements LongString {

        private final String text;

        private ArrayLongString(String text) {
            this.text = text;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String asString() throws IllegalStateException {
            return this.text;
        }

        @Override
        public FileChannel openReadOnlyChannel() {
            throw new IllegalStateException(String.format("Not support %s", FileChannel.class.getName()));
        }

    }

    private static final class PathLongString implements LongString {

        private final Path path;

        private PathLongString(Path path) {
            this.path = path;
            TempFiles.addTempPath(path);
        }

        @Override
        protected void finalize() {
            try {
                TempFiles.removeTempPath(this.path);
                Files.deleteIfExists(this.path);
            } catch (Throwable e) {
                //here don't need throw exception
            }
        }

        @Override
        public boolean isString() {
            return false;
        }

        @Override
        public String asString() throws IllegalStateException {
            throw new IllegalStateException(String.format("Not support %s", String.class.getName()));
        }

        @Override
        public FileChannel openReadOnlyChannel() throws IOException {
            return FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
        }


    }


}
