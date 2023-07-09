package io.jdbd.type;


import java.nio.file.Path;

public interface BlobPath extends PathParameter {


    static BlobPath from(boolean deleteOnClose, Path path) {
        return JdbdTypes.blobPathParam(deleteOnClose, path);
    }


}
