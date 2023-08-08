package io.jdbd.session;

import io.jdbd.lang.Nullable;

/**
 * <p>
 * This interface is base interface of following :
 *     <ul>
 *         <li>{@link DatabaseSessionFactory}</li>
 *         <li>{@link SessionMetaSpec}</li>
 *         <li>{@link io.jdbd.statement.Statement}</li>
 *         <li>{@link io.jdbd.result.ResultItem}</li>
 *         <li>{@link io.jdbd.VersionSpec}</li>
 *         <li>{@link io.jdbd.result.ServerException}</li>
 *         <li>{@link io.jdbd.result.RefCursor}</li>
 *         <li>{@link TransactionOption}</li>
 *         <li>{@link io.jdbd.result.Warning}</li>
 *         <li>{@link ChunkOption}</li>
 *     </ul>
 *     ,it provider more dialectal driver.
 * </p>
 *
 * @see Option
 * @see io.jdbd.statement.Statement#setOption(Option, Object)
 * @since 1.0
 */
public interface OptionSpec {

    /**
     * @return null or the value of option.
     */
    @Nullable
    <T> T valueOf(Option<T> option);


}
