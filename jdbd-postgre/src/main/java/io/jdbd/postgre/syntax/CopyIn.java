package io.jdbd.postgre.syntax;

import org.reactivestreams.Publisher;

import java.nio.file.Path;
import java.util.function.Function;

public interface CopyIn {

    Mode getMode();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn' {@link Mode#FILE}
     */
    Path getPath();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn't {@link Mode#PROGRAM}
     */
    String getCommand();

    /**
     * @throws IllegalStateException when {@link #getMode()} isn't one of :
     *                               <ul>
     *                                   <li>{@link Mode#PROGRAM}</li>
     *                                   <li>{@link Mode#STDIN}</li>
     *                               </ul>
     */
    Function<String, Publisher<byte[]>> getFunction();


    enum Mode {
        FILE,
        PROGRAM,
        STDIN
    }

}
