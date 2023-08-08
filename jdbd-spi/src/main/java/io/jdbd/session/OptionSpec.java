package io.jdbd.session;

import io.jdbd.lang.Nullable;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link DatabaseSessionFactory}</li>
 *         <li>{@link SessionMetaSpec}</li>
 *         <li>{@link io.jdbd.statement.Statement}</li>
 *         <li>{@link io.jdbd.VersionSpec}</li>
 *         <li>{@link io.jdbd.result.ServerException}</li>
 *         <li>{@link io.jdbd.result.RefCursor}</li>
 *         <li>{@link TransactionOption}</li>
 *         <li>{@link io.jdbd.result.Warning}</li>
 *         <li>{@link ChunkOption}</li>
 *     </ul>
 * </p>
 *
 * @since 1.0
 */
public interface OptionSpec {

    @Nullable
    <T> T valueOf(Option<T> option);


}
