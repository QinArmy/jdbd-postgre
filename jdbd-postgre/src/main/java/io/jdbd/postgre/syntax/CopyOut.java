package io.jdbd.postgre.syntax;

import java.nio.file.Path;

public interface CopyOut {

    Mode getMode();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn' {@link CopyIn.Mode#FILE}
     */
    Path getPath();


    enum Mode {
        FILE,
        PROGRAM,
        STDOUT
    }
}
