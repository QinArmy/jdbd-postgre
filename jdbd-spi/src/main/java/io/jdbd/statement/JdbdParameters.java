package io.jdbd.statement;

import io.jdbd.lang.NonNull;
import io.jdbd.lang.Nullable;
import org.reactivestreams.Publisher;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Objects;

/**
 * <p>
 * This class provider the method create {@link Parameter}
 * </p>
 *
 * @since 1.0
 */
abstract class JdbdParameters {

    private JdbdParameters() {
        throw new UnsupportedOperationException();
    }

    static OutParameter outParam(@Nullable String name, @Nullable Object value) {
        if (name == null) {
            throw new NullPointerException("out parameter name must non-null");
        }
        return new JdbdOutParameter(name, value);
    }

    static Blob blobParam(@Nullable Publisher<byte[]> source) {
        if (source == null) {
            throw new NullPointerException("source must non-null");
        }
        return new JdbdBlob(source);
    }

    static Clob clobParam(@Nullable Publisher<CharSequence> source) {
        if (source == null) {
            throw new NullPointerException("source must non-null");
        }
        return new JdbdClob(source);
    }

    static Text textParam(@Nullable Charset charset, @Nullable Publisher<byte[]> source) {
        if (charset == null) {
            throw new NullPointerException("charset must non-null");
        } else if (source == null) {
            throw new NullPointerException("source must non-null");
        }
        return new JdbdText(charset, source);
    }

    static TextPath textPathParam(boolean deleteOnClose, @Nullable Charset charset, @Nullable Path path) {
        if (charset == null) {
            throw new NullPointerException("charset must non-null");
        } else if (path == null) {
            throw new NullPointerException("path must non-null");
        }
        return new JdbdTextPath(deleteOnClose, charset, path);
    }

    static BlobPath blobPathParam(boolean deleteOnClose, @Nullable Path path) {
        if (path == null) {
            throw new NullPointerException("path must non-null");
        }
        return new JdbdBlobPath(deleteOnClose, path);
    }

    /**
     * <p>
     * This class is standard implementation of {@link OutParameter}.
     * </p>
     *
     * @since 1.0
     */
    private static final class JdbdOutParameter implements OutParameter {


        private final String name;


        private final Object value;

        /**
         * private constructor
         */
        private JdbdOutParameter(String name, @Nullable Object value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public Object value() {
            return this.value;
        }


        @Override
        public int hashCode() {
            return Objects.hash(this.name, this.value);
        }

        @Override
        public boolean equals(final Object obj) {
            final boolean match;
            if (obj == this) {
                match = true;
            } else if (obj instanceof JdbdOutParameter) {
                final JdbdOutParameter o = (JdbdOutParameter) obj;
                match = o.name.equals(this.name) && Objects.equals(o.value, this.value);
            } else {
                match = false;
            }
            return match;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append(this.getClass().getName())
                    .append("[ name : ")
                    .append(this.name)
                    .append(" , value : ");
            if (this.value instanceof String) {
                builder.append('?');
            } else {
                builder.append(this.value);
            }
            return builder.append(" , hash : ")
                    .append(System.identityHashCode(this))
                    .append(" ]")
                    .toString();
        }


    }//JdbdOutParameter


    private static final class JdbdBlob implements Blob {

        private final Publisher<byte[]> source;

        private JdbdBlob(Publisher<byte[]> source) {
            this.source = source;
        }

        @NonNull
        @Override
        public Publisher<byte[]> value() {
            return this.source;
        }


    }//JdbdBlob

    private static final class JdbdClob implements Clob {

        private final Publisher<CharSequence> source;

        private JdbdClob(Publisher<CharSequence> source) {
            this.source = source;
        }

        @NonNull
        @Override
        public Publisher<CharSequence> value() {
            return this.source;
        }


    }//JdbdBlob


    private static final class JdbdText implements Text {

        private final Charset charset;

        private final Publisher<byte[]> source;

        private JdbdText(Charset charset, Publisher<byte[]> source) {
            this.charset = charset;
            this.source = source;
        }

        @Override
        public Charset charset() {
            return this.charset;
        }


        @NonNull
        @Override
        public Publisher<byte[]> value() {
            return this.source;
        }

        @Override
        public String toString() {
            return String.format("%s[ charset : %s ]", getClass().getName(), this.charset.name());
        }


    }//JdbdText


    private static final class JdbdTextPath implements TextPath {

        private final boolean deleteOnClose;

        private final Charset charset;

        private final Path path;

        private JdbdTextPath(boolean deleteOnClose, Charset charset, Path path) {
            this.deleteOnClose = deleteOnClose;
            this.charset = charset;
            this.path = path;
        }

        @Override
        public Charset charset() {
            return this.charset;
        }

        @Override
        public boolean isDeleteOnClose() {
            return this.deleteOnClose;
        }

        @NonNull
        @Override
        public Path value() {
            return this.path;
        }

        @Override
        public String toString() {
            return String.format("%s[ deleteOnClose : %s , charset : %s , path : %s]",
                    getClass().getName(), this.deleteOnClose, this.charset.name(), this.path
            );
        }


    }//JdbdTextPath

    private static final class JdbdBlobPath implements BlobPath {

        private final boolean deleteOnClose;

        private final Path path;

        private JdbdBlobPath(boolean deleteOnClose, Path path) {
            this.deleteOnClose = deleteOnClose;
            this.path = path;
        }

        @Override
        public boolean isDeleteOnClose() {
            return this.deleteOnClose;
        }

        @NonNull
        @Override
        public Path value() {
            return this.path;
        }

        @Override
        public String toString() {
            return String.format("%s[ deleteOnClose : %s , path : %s]",
                    getClass().getName(), this.deleteOnClose, this.path
            );
        }


    }//JdbdBlobPath


}
