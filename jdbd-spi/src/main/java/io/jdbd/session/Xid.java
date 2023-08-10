package io.jdbd.session;

import io.jdbd.lang.Nullable;

import java.util.Map;

/**
 * <p>
 * xid output : gtrid [, bqual [, formatID ]]
 * </p>
 * <p>
 * To be safe,{@link RmDatabaseSession} write gtrid and bqual as hex strings. steps :
 * <ul>
 *     <li>Get byte[] with {@link java.nio.charset.StandardCharsets#UTF_8}</li>
 *     <li>write gtrid or bqual as hex strings</li>
 * </ul>
 * the conversion process of {@link RmDatabaseSession#recover(int, Map)} is the reverse of above.
 * </p>
 *
 * @see Option#XID
 * @see RmDatabaseSession
 */
public interface Xid {

    /**
     * <p>
     * The global transaction identifier string
     * </p>
     * <p>
     *   <ul>
     *       <li>Global transaction identifier must have text</li>
     *   </ul>
     * </p>
     *
     * @return a global transaction identifier.
     */
    String getGtrid();

    /**
     * Obtain the transaction branch identifier part of XID as an string.
     * <p>
     *   <ul>
     *       <li>If non-null,branch transaction identifier must have text</li>
     *   </ul>
     * </p>
     *
     * @return a branch qualifier
     */
    @Nullable
    String getBqual();

    /**
     * Obtain the format identifier part of the XID.
     *
     * @return Format identifier. O means the OSI CCR format.
     */
    int getFormatId();

    /**
     * <p>
     * The implementation of {@link Xid} must correctly override this method with :
     *     <ul>
     *         <li>{@link #getGtrid()}</li>
     *         <li>{@link #getBqual()}</li>
     *         <li>{@link #getFormatId()}</li>
     *     </ul>
     * </p>
     * <p>
     *     Like following :
     *     <pre>
     *         <code><br/>
     *               &#64;Override
     *               public int hashCode() {
     *                   return Objects.hash(this.gtrid, this.bqual, this.formatId);
     *               }
     *         </code>
     *     </pre>
     * </p>
     */
    @Override
    int hashCode();

    /**
     * <p>
     * The implementation of {@link Xid} must correctly override this method with :
     *     <ul>
     *         <li>{@link #getGtrid()}</li>
     *         <li>{@link #getBqual()}</li>
     *         <li>{@link #getFormatId()}</li>
     *     </ul>
     * </p>
     * <p>
     *     Like following :
     *     <pre>
     *         <code><br/>
     *           &#64;Override
     *           public boolean equals(final Object obj) {
     *               final boolean match;
     *               if (obj == this) {
     *                   match = true;
     *               } else if (obj instanceof JdbdXid) {
     *                   final JdbdXid o = (JdbdXid) obj;
     *                   match = o.gtrid.equals(this.gtrid)
     *                           &amp;&amp; Objects.equals(o.bqual, this.bqual)
     *                           &amp;&amp; o.formatId == this.formatId;
     *               } else {
     *                   match = false;
     *               }
     *               return match;
     *           }
     *         </code>
     *     </pre>
     * </p>
     */
    @Override
    boolean equals(Object obj);

    /**
     * override {@link Object#toString()}
     *
     * @return xid info, contain : <ol>
     * <li>class name</li>
     * <li>{@link #getGtrid()}</li>
     * <li>{@link #getBqual()}</li>
     * <li>{@link #getFormatId()}</li>
     * <li>{@link System#identityHashCode(Object)}</li>
     * </ol>
     */
    @Override
    String toString();

    /**
     * <p>
     * Create one {@link Xid} instance.
     * </p>
     *
     * @param gtrid must have text
     * @param bqual null or must have text
     * @throws IllegalArgumentException throw when gtrid or bqual error.
     */
    static Xid from(String gtrid, @Nullable String bqual, int formatId) {
        return JdbdXid.from(gtrid, bqual, formatId);
    }

}
