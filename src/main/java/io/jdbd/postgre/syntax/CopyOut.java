package io.jdbd.postgre.syntax;

import java.nio.file.Path;

public interface CopyOut extends CopyOperation {

    Mode getMode();

    /**
     * @return bind index or -1 .
     */
    int getBindIndex();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn' {@link CopyIn.Mode#FILE}
     */
    Path getPath();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn't {@link CopyIn.Mode#PROGRAM} or {@link #getBindIndex()}  isn' {@code -1}.
     */
    String getCommand();


    enum Mode {
        FILE,
        PROGRAM,
        STDOUT
    }

}
