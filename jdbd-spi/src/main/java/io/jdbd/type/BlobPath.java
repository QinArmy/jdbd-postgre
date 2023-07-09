package io.jdbd.type;


import io.jdbd.statement.PathParameter;

import java.nio.file.Path;

public interface BlobPath extends PathParameter {


    static BlobPath from(boolean deleteOnClose, Path path) {
        return JdbdTypes.blobPathParam(deleteOnClose, path);
    }


}
