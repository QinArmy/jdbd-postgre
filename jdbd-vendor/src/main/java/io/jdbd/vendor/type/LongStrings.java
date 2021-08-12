package io.jdbd.vendor.type;

import io.jdbd.type.geometry.LongString;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

public abstract class LongStrings implements LongString {


    public static LongString fromString(String text) {
        return new StringLongString(Objects.requireNonNull(text, "text"));
    }

    /**
     * @param path should in {@code java.io.tmpdir} directory or sub directory.
     */
    public static LongString fromTempPath(Path path) {
        return new PathLongString(Objects.requireNonNull(path, "path"));
    }


    static IllegalStateException creteNotSupportStringError() {
        return new IllegalStateException("Non-underlying String,use openReadOnlyChannel() method.");
    }

    static IllegalStateException creteNotSupportFileChannel() {
        return new IllegalStateException("Non-underlying file,use asString() method.");
    }


    private LongStrings() {
    }


    private static final class StringLongString implements LongString {

        private final String text;

        private StringLongString(String text) {
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
            throw creteNotSupportFileChannel();
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
            throw creteNotSupportStringError();
        }

        @Override
        public FileChannel openReadOnlyChannel() throws IOException {
            return FileChannel.open(this.path, StandardOpenOption.READ, StandardOpenOption.DELETE_ON_CLOSE);
        }


    }


}
