package io.jdbd.type;

import io.jdbd.lang.NonNull;

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
public interface PathParameter extends LongParameter {

    /**
     * @see java.nio.file.StandardOpenOption#DELETE_ON_CLOSE
     */
    boolean isDeleteOnClose();


    @NonNull
    @Override
    Path value();


}
