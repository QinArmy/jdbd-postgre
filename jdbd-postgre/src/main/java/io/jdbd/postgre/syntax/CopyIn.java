package io.jdbd.postgre.syntax;

import java.nio.file.Path;

public interface CopyIn {

    Mode getMode();

    /**
     * @return bind index or -1 .
     */
    int getBindIndex();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn' {@link Mode#FILE} or {@link #getBindIndex()} isn' {@code -1}.
     */
    Path getPath();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn't {@link Mode#PROGRAM} or {@link #getBindIndex()}  isn' {@code -1}.
     */
    String getCommand();


    enum Mode {
        FILE,
        PROGRAM,
        STDIN
    }

}
