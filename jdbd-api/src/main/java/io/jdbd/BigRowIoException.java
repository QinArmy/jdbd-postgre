package io.jdbd;

import java.io.IOException;
import java.nio.file.Path;

public class BigRowIoException extends JdbdIoException {

    private final Path bigRowFile;

    public BigRowIoException(String message, Throwable cause) {
        super(message, cause);
        this.bigRowFile = null;
    }

    public BigRowIoException(String message, Path bigRowFile) {
        super(message);
        this.bigRowFile = bigRowFile;
    }

    public BigRowIoException(String message, IOException cause, Path bigRowFile) {
        super(message, cause);
        this.bigRowFile = bigRowFile;
    }

    public BigRowIoException(String message, IOException cause, boolean enableSuppression
            , boolean writableStackTrace, Path bigRowFile) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.bigRowFile = bigRowFile;
    }

    public Path getBigRowFile() {
        return this.bigRowFile;
    }
}
