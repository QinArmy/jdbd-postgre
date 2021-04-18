package io.jdbd.stmt;

import io.jdbd.JdbdNonSQLException;

import java.nio.file.Path;

public class LocalFileException extends JdbdNonSQLException {

    private final Path localFile;

    private final long sentBytes;

    public LocalFileException(Path localFile, String messageFormat, Object... args) {
        super(messageFormat, args);
        this.localFile = localFile;
        this.sentBytes = 0L;
    }

    public LocalFileException(Throwable cause, Path localFile, long sentBytes, String messageFormat, Object... args) {
        super(cause, messageFormat, args);
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
