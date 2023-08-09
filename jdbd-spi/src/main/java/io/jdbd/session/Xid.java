package io.jdbd.session;

import io.jdbd.lang.Nullable;

/**
 * @see RmDatabaseSession
 */
public interface Xid extends OptionSpec {

    /**
     * <p>
     * The length that The global transaction identifier byte[]
     * </p>
     *
     * @return a global transaction identifier
     */
    String getGtrid();

    @Nullable
    String getBqual();

    int getFormatId();

    @Override
    int hashCode();

    @Override
    boolean equals(Object obj);

    @Override
    String toString();

}
