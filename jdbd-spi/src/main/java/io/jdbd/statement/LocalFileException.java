package io.jdbd.statement;

import io.jdbd.JdbdNonSQLException;
import io.jdbd.lang.Nullable;

import java.nio.file.Path;

@Deprecated
public class LocalFileException extends JdbdNonSQLException {

    private final Path localFile;

    private final long sentBytes;

    public LocalFileException(Path localFile, String message) {
        super(message);
        this.localFile = localFile;
        this.sentBytes = 0L;
    }

    public LocalFileException(Path localFile, String message, @Nullable Throwable cause) {
        super(message, cause);
        this.localFile = localFile;
        this.sentBytes = 0L;
    }

    public LocalFileException(Path localFile, long sentBytes, String message, @Nullable Throwable cause) {
        super(message, cause);
        this.localFile = localFile;
        this.sentBytes = sentBytes;
    }


    public final Path getLocalFile() {
        return this.localFile;
    }

    public final long getSentBytes() {
        return this.sentBytes;
    }
}
