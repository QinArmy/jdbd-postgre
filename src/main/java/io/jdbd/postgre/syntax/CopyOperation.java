package io.jdbd.postgre.syntax;

import java.nio.file.Path;

public interface CopyOperation {

    /**
     * @return bind index or -1 .
     */
    int getBindIndex();

    /**
     * @throws IllegalStateException when {@code #getMode()} isn' {@code Mode#FILE}
     */
    Path getPath();

    /**
     * @throws IllegalStateException when {@code #getMode()} isn't {@code Mode#PROGRAM} or {@link #getBindIndex()}  isn' {@code -1}.
     */
    String getCommand();

}
