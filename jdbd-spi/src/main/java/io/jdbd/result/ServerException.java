package io.jdbd.result;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.session.Option;


/**
 * <p>
 * Emit(not throw), when server return error message.
 * </p>
 *
 * @since 1.0
 */
public abstract class ServerException extends JdbdException {


    protected ServerException(String message, @Nullable String sqlState, int vendorCode) {
        super(message, sqlState, vendorCode);
    }

    /**
     * <p>
     * Get one field value of server error message.
     * </p>
     *
     * @return null , if field not exists.
     * @see Option#MESSAGE
     * @see Option#SQL_STATE
     * @see Option#VENDOR_CODE
     */
    @Nullable
    public abstract <T> T valueOf(Option<T> option);


    @Override
    public abstract String toString();


}
