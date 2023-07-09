package io.jdbd.type;


import java.nio.charset.Charset;
import java.nio.file.Path;

public interface TextPath extends PathParameter {

    Charset charset();


    static TextPath from(boolean deleteOnClose, Charset charset, Path path) {
        return JdbdTypes.textPathParam(deleteOnClose, charset, path);
    }

}
