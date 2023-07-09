package io.jdbd.statement;

import io.jdbd.lang.NonNull;
import io.jdbd.type.BlobPath;
import io.jdbd.type.TextPath;

import java.nio.file.Path;

/**
 * <p>
 * This interface is only base interface of following :
 *     <ul>
 *         <li>{@link BlobPath}</li>
 *         <li>{@link TextPath}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface PathParameter extends Parameter {

    /**
     * @see java.nio.file.StandardOpenOption#DELETE_ON_CLOSE
     */
    boolean isDeleteOnClose();


    @NonNull
    @Override
    Path value();


}
