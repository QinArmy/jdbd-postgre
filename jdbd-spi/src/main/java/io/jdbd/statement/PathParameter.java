package io.jdbd.statement;

import io.jdbd.lang.NonNull;

import java.nio.file.Path;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link BlobPath}</li>
 *         <li>{@link TextPath}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface PathParameter extends Parameter {


    boolean isDeleteOnClose();


    @NonNull
    @Override
    Path value();


}
