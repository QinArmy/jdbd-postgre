package io.jdbd.statement;


import java.nio.file.Path;

public interface BlobPath extends PathParameter {


    static BlobPath from(boolean deleteOnClose, Path path) {
        return JdbdParameters.blobPathParam(deleteOnClose, path);
    }


}
